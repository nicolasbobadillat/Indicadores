import yfinance as yf
import pandas as pd
import requests
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession

def init_spark():
    return SparkSession.builder.appName("MarketGuardian_V60_FinalData").master("local[*]").getOrCreate()

def get_macro_real():
    print("🌍 Descargando Macro Data (Híbrido: Mindicador + Yahoo + RapidAPI)...")
    data = []

    # 1. API MINDICADOR.CL (Chile Oficial)
    try:
        resp_cl = requests.get('https://mindicador.cl/api').json()

        # USD
        usd_val = float(resp_cl['dolar']['valor'])
        # Calculamos variación simulada o 0.0 si la API no la entrega directo
        data.append({'Indicador': 'USD', 'Valor': usd_val, 'Variacion': 0.0})

        # UF
        uf_val = float(resp_cl['uf']['valor'])
        data.append({'Indicador': 'UF', 'Valor': uf_val, 'Variacion': 0.0})
    except Exception as e:
        print(f"⚠️ Error Mindicador: {e}")
        data.append({'Indicador': 'USD', 'Valor': 980.0, 'Variacion': 0.0})
        data.append({'Indicador': 'UF', 'Valor': 36900.0, 'Variacion': 0.0})

    # 2. API FEAR & GREED (RapidAPI)
    try:
        url = "https://fear-and-greed-index.p.rapidapi.com/v1/fgi"
        headers = {
            "x-rapidapi-key": "c5f1fefbc9mshee2eb77c59a5aeap10950bjsn3bedb5eaf84b", # <--- PEGA TU API KEY AQUÍ
            "x-rapidapi-host": "fear-and-greed-index.p.rapidapi.com"
        }
        resp_fgi = requests.get(url, headers=headers).json()

        # Estructura usual: {'fgi': {'now': {'value': 50, 'valueText': 'Neutral'}}}
        fgi_val = float(resp_fgi['fgi']['now']['value'])
        data.append({'Indicador': 'F&G Index', 'Valor': fgi_val, 'Variacion': 0.0})
    except Exception as e:
        print(f"⚠️ Error Fear&Greed: {e}")
        data.append({'Indicador': 'F&G Index', 'Valor': 50.0, 'Variacion': 0.0})

    # 3. YAHOO FINANCE (Resto del Mundo)
    tickers = {
        'S&P500': '^GSPC', 'NASDAQ': '^NDX', 'DOW': '^DJI',
        'IPSA': '^IPSA', 'BTC': 'BTC-USD', 'VIX': '^VIX', 'GOLD': 'GC=F'
    }

    for name, sym in tickers.items():
        try:
            hist = yf.Ticker(sym).history(period="2d")
            if len(hist) > 1:
                val = float(hist['Close'].iloc[-1])
                prev = float(hist['Close'].iloc[-2])
                var = ((val - prev) / prev) * 100
            else:
                val = float(hist['Close'].iloc[-1])
                var = 0.0
            data.append({'Indicador': name, 'Valor': val, 'Variacion': var})
        except:
            data.append({'Indicador': name, 'Valor': 0.0, 'Variacion': 0.0})

    return pd.DataFrame(data)

def smart_download(ticker):
    stock = yf.Ticker(ticker)
    try: hist = stock.history(period="2y", auto_adjust=True)
    except: hist = pd.DataFrame()

    # PROXY FIX (IPSA)
    if (hist.empty or len(hist) < 10) and ("IPSA" in ticker):
        print(f"⚠️ Usando Proxy ECH para {ticker}...")
        try:
            proxy = yf.Ticker("ECH")
            hist_proxy = proxy.history(period="2y", auto_adjust=True)
            if not hist_proxy.empty:
                factor = 10223.0 / hist_proxy['Close'].iloc[-1]
                hist = hist_proxy.copy()
                hist['Close'] = hist['Close'] * factor
                hist['Open'] = hist['Open'] * factor
                hist['High'] = hist['High'] * factor
                hist['Low'] = hist['Low'] * factor
        except: pass
    return stock, hist

def get_data_institutional(tickers):
    snapshot_data = []
    history_data = []

    print("🚀 Iniciando Extracción Final (Data Repair)...")

    for t in tickers:
        try:
            print(f"Analizando: {t}...")
            stock, hist_2y = smart_download(t)

            if hist_2y.empty: continue

            # 1. HISTORIA
            hist_2y.index = pd.to_datetime(hist_2y.index).tz_localize(None)
            hist_2y['SMA_20'] = hist_2y['Close'].rolling(window=20).mean()
            hist_2y['SMA_50'] = hist_2y['Close'].rolling(window=50).mean()
            hist_2y['SMA_200'] = hist_2y['Close'].rolling(window=200).mean()

            hist_1y = hist_2y.iloc[-252:].copy()

            df_p = hist_1y[['Close', 'SMA_20', 'SMA_50', 'SMA_200']].reset_index()
            df_p.columns = ['Date', 'Close', 'SMA_20', 'SMA_50', 'SMA_200']
            df_p['Ticker'] = t
            df_p['Date'] = df_p['Date'].dt.strftime('%Y-%m-%d')
            df_p['Type'] = 'Price'
            df_p['Metric_Revenue'] = 0.0
            df_p['Metric_NetIncome'] = 0.0
            history_data.extend(df_p.to_dict('records'))

            # 2. FUNDAMENTALES (FIX)
            try: info = stock.info
            except: info = {}

            precio = hist_1y['Close'].iloc[-1]

            # Performance
            try:
                p_1y = hist_2y['Close'].iloc[-252]
                perf_1y = ((precio - p_1y)/p_1y)*100
                y_start = pd.Timestamp(datetime.now().year, 1, 1)
                h_ytd = hist_2y[hist_2y.index >= y_start]
                perf_ytd = ((precio - h_ytd.iloc[0]['Close'])/h_ytd.iloc[0]['Close'])*100 if not h_ytd.empty else 0
            except: perf_1y, perf_ytd = 0.0, 0.0

            is_index = t.startswith('^') or 'BTC' in t or 'IPSA' in t

            # VALORES SEGUROS
            rev, net, ebitda = 0, 0, 0
            pe, peg, beta, quick = 0, 0, 1.0, 0
            curr_assets, curr_liab = 0, 1
            roe = 0

            if not is_index:
                # Extraccion Info
                rev = info.get('totalRevenue', 0)
                net = info.get('netIncomeToCommon', 0)
                ebitda = info.get('ebitda', 0)
                pe = info.get('trailingPE', 0)

                # FIX PEG
                peg = info.get('pegRatio')
                if peg is None: peg = info.get('trailingPegRatio', 0)

                # FIX QUICK RATIO
                quick = info.get('quickRatio')

                # FIX ROE (Priorizar dato ya calculado)
                roe = info.get('returnOnEquity', 0) * 100

                # Fallbacks manuales si info falla
                inc = stock.income_stmt
                bs = stock.balance_sheet

                def gv(df, k):
                    if df is None or df.empty: return 0
                    for x in k:
                        if x in df.index: return float(df.loc[x].iloc[0])
                    return 0

                if rev == 0: rev = gv(inc, ['Total Revenue', 'Revenue'])
                if net == 0: net = gv(inc, ['Net Income'])
                if ebitda == 0: ebitda = gv(inc, ['Normalized EBITDA', 'EBITDA'])

                curr_assets = gv(bs, ['Current Assets'])
                curr_liab = gv(bs, ['Current Liabilities'])
                inventory = gv(bs, ['Inventory', 'Inventories'])
                equity = gv(bs, ['Stockholders Equity'])

                # Calculos manuales de rescate
                if quick is None and curr_liab > 0:
                    quick = (curr_assets - inventory) / curr_liab
                if roe == 0 and equity > 0:
                    roe = (net / equity) * 100
                if pe == 0 and net > 0:
                    # PE estimado
                    shares = info.get('sharesOutstanding', 1)
                    eps = net / shares
                    if eps > 0: pe = precio / eps

            # Clean Final Values
            if peg is None: peg = 0
            if quick is None: quick = 0

            # 52W Range
            h52 = info.get('fiftyTwoWeekHigh', hist_1y['Close'].max())
            l52 = info.get('fiftyTwoWeekLow', hist_1y['Close'].min())

            # Dividend
            div_yield = info.get('dividendYield')
            if div_yield is None:
                dr = info.get('dividendRate')
                div_yield = (dr/precio) if (dr and precio) else 0.0
            if div_yield > 0.20: div_yield /= 100

            # Score
            rec_mean = info.get('recommendationMean')
            score = (6.0 - rec_mean) if rec_mean else 3.0

            snapshot_data.append({
                "Ticker": t, "Precio": precio,
                "Sector": info.get('sector', 'Index/Fund'), "Industria": info.get('industry', 'Market'),
                "Beta": info.get('beta', 1.0), "PE_Ratio": pe, "PEG_Ratio": peg, "Quick_Ratio": quick,
                "Target_Price": info.get('targetMeanPrice', precio), "Recommendation": info.get('recommendationKey','none'),
                "Consensus_Score": score, "Num_Analysts": info.get('numberOfAnalystOpinions',0),
                "Dividend_Yield": div_yield,
                "52W_High": float(h52), "52W_Low": float(l52),
                "Perf_YTD": perf_ytd, "Perf_1Y": perf_1y,
                "Ventas": rev, "EBITDA": ebitda, "Utilidad_Neta": net,
                "Activo_Cte": curr_assets, "Pasivo_Cte": curr_liab, "Patrimonio": 0,
                "ROE": roe, "Costo_Ventas": gv(inc, ['Cost Of Revenue']), "OpEx": gv(inc, ['Operating Expense'])
            })

            # Historia Financiera (Para gráficos)
            if not is_index:
                inc = stock.income_stmt if not stock.income_stmt.empty else stock.financials
                if not inc.empty:
                    for i in range(min(3, len(inc.columns))):
                        d = str(inc.columns[i].year) if hasattr(inc.columns[i], 'year') else str(inc.columns[i])[:4]
                        history_data.append({
                            'Ticker': t, 'Date': d, 'Type': 'Financials',
                            'Metric_Revenue': gv(inc.iloc[:, [i]], ['Total Revenue', 'Revenue']),
                            'Metric_NetIncome': gv(inc.iloc[:, [i]], ['Net Income']),
                            'Close':0, 'SMA_20':0, 'SMA_50':0, 'SMA_200':0
                        })

        except Exception as e:
            print(f"❌ Error en {t}: {e}")

    # 3. BENCHMARKS
    benchs = ['^GSPC', '^IPSA']
    for b in benchs:
        if b not in tickers:
            _, h = smart_download(b)
            if not h.empty:
                h.index = pd.to_datetime(h.index).tz_localize(None)
                d = h['Close'].reset_index()
                d.columns = ['Date', 'Close']
                d['Date'] = d['Date'].dt.strftime('%Y-%m-%d')
                d['Ticker'] = b
                d['Type'] = 'Benchmark'
                d['SMA_20']=0;d['SMA_50']=0;d['SMA_200']=0;d['Metric_Revenue']=0;d['Metric_NetIncome']=0
                history_data.extend(d.to_dict('records'))

    return snapshot_data, history_data

def run_etl():
    spark = init_spark()
    tickers = ['AAPL', 'MSFT', 'NVDA', 'SQM-B.SN', 'BSANTANDER.SN', '^IPSA', '^GSPC']
    snap, hist = get_data_institutional(tickers)
    macro = get_macro_real()
    pd.DataFrame(snap).to_csv("data_kpi.csv", index=False)
    pd.DataFrame(hist).to_csv("data_history.csv", index=False)
    macro.to_csv("data_macro.csv", index=False)
    print("💾 Datos V60 (Final Fix) Generados.")

if __name__ == "__main__":
    run_etl()
