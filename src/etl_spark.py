
"""ETL Pipeline - Terminal Inversionista 
-----------------------------------------------------------

Descripción:
    Pipeline híbrido de extracción y transformación de datos financieros.
    1. EXTRACT: Usa yfinance (API) para datos de mercado y fundamentales.
    2. TRANSFORM: Usa PySpark para imponer esquemas estrictos y calcular columnas derivadas.
    3. LOAD: Publica archivos CSV estáticos (Patrón Data Lake) para consumo del Frontend.

Tecnologías: Python, Pandas, PySpark, Yahoo Finance API.
"""

import os
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ==========================================
# 1. CONFIGURACIÓN Y CONSTANTES
# ==========================================



# Universo de Activos a Monitorear
TARGET_TICKERS = [
    #TOP
    'NVDA', 'GOOGL','PLTR','SOFI',
    # Índices & Macro
    '^GSPC','^IPSA', '^NDX', '^DJI',
    # Magnificent 7 (Tech)
    'AAPL', 'MSFT','AMZN', 'META', 'TSLA', 'AMD',
    # Chile Blue Chips
    'BCI.SN', 'BSANTANDER.SN', 'CHILE.SN','ITAUCL.SN', 'COPEC.SN',
    'CENCOSUD.SN', 'FALABELLA.SN', 'SQM-B.SN','BICE.SN', 
    # Financials & Defensive
    'JPM', 'BAC', 'KO', 'PEP', 'MCD', 'WMT',
    # Commodities & Crypto
    'BTC-USD', 'ETH-USD'
]

MACRO_INDICATORS = {
    'S&P500': '^GSPC', 'NASDAQ': '^NDX', 'IPSA': '^IPSA', 
    'BTC': 'BTC-USD', 'VIX': '^VIX', 'ORO': 'GC=F', 
    'DOW': '^DJI', 'COBRE': 'HG=F'
}

TICKER_MAPPING = {
    '^GSPC': 'S&P 500',
    '^NDX':  'Nasdaq 100',
    '^DJI':  'Dow Jones',
    'BTC-USD': 'BTC',
    'ETH-USD': 'ETH'
}

# ==========================================
# 2. INICIALIZACIÓN DE SPARK
# ==========================================

def init_spark() -> SparkSession:
    """
    Inicializa la sesión de Spark con optimización Arrow habilitada.
    
    Returns:
        SparkSession: Objeto de sesión configurado para procesamiento local.
    """
    return SparkSession.builder \
        .appName("MarketGuardian_ETL_Institutional") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()

def get_api_key():
    # Busca la variable de entorno que definimos en el YAML
    api_key = os.getenv("FMP_API_KEY") 
    
    # Si no la encuentra, lanza un error claro en lugar de buscar DEFAULT_API_KEY
    if not api_key:
        raise ValueError("❌ ERROR FATAL: No se encontró la API Key en las variables de entorno (GitHub Secrets).")
    
    return api_key

# ==========================================
# 3. CAPA DE EXTRACCIÓN (EXTRACT)
# ==========================================

def get_macro_data(api_key: str) -> list:
    """
    Extrae indicadores macroeconómicos globales y locales (Chile).
    Usa 'Fast Info' de Yahoo para reducir latencia en tiempo real.

    Args:
        api_key (str): Key para la API de Fear & Greed Index.

    Returns:
        list: Lista de diccionarios con {Indicador, Valor, Variacion}.
    """
    print("🌍 Extrayendo Datos Macro (Strategy: Fast Info + API Mindicador)...")
    data = []

    # A. API MINDICADOR (Datos Chile)
    local_indicators = {'USD': 'dolar', 'UF': 'uf', 'EUR': 'euro'}
    for ind_name, ind_code in local_indicators.items():
        try:
            resp = requests.get(f'https://mindicador.cl/api/{ind_code}', timeout=5).json()
            serie = resp.get('serie', [])
            if len(serie) >= 2:
                curr_val = float(serie[0]['valor'])
                prev_val = float(serie[1]['valor'])
                var = ((curr_val - prev_val) / prev_val) * 100
            else:
                curr_val = float(serie[0]['valor']) if serie else 0.0
                var = 0.0
            data.append({'Indicador': ind_name, 'Valor': curr_val, 'Variacion': var})
        except Exception:
            # Fallback seguro visual
            safe_val = 980.0 if ind_name == 'USD' else 37000.0
            data.append({'Indicador': ind_name, 'Valor': safe_val, 'Variacion': 0.0})

    # B. API FEAR & GREED (Sentimiento de Mercado)
    try:
        url = "https://fear-and-greed-index.p.rapidapi.com/v1/fgi"
        headers = {"x-rapidapi-key": api_key, "x-rapidapi-host": "fear-and-greed-index.p.rapidapi.com"}
        resp_fgi = requests.get(url, headers=headers, timeout=5).json()
        fgi_val = float(resp_fgi['fgi']['now']['value'])
        data.append({'Indicador': 'F&G Index', 'Valor': fgi_val, 'Variacion': 0.0})
    except Exception:
        data.append({'Indicador': 'F&G Index', 'Valor': 50.0, 'Variacion': 0.0})

    # C. YAHOO FINANCE (Global Markets)
    for name, sym in MACRO_INDICATORS.items():
        try:
            ticker = yf.Ticker(sym)
            val, var = 0.0, 0.0
            use_history = (name == 'BTC') # Crypto requiere historial por volatilidad

            # Intento 1: Fast Info (Menor Latencia)
            if not use_history:
                try:
                    fast = ticker.fast_info
                    last_price = fast.last_price
                    prev_close = fast.regular_market_previous_close
                    if last_price and prev_close:
                        val = float(last_price)
                        var = ((last_price - prev_close) / prev_close) * 100
                    else:
                        use_history = True
                except:
                    use_history = True

            # Intento 2: Histórico (Fallback o BTC)
            if use_history:
                hist = ticker.history(period="5d")
                
                # Lógica Proxy para IPSA (Si Yahoo falla con el índice local, usamos el ETF ECH)
                if name == 'IPSA' and (hist.empty or len(hist) < 2 or hist['Close'].iloc[-1] == 0):
                    print("   ⚠️ IPSA sin data, activando proxy ECH...")
                    try:
                        proxy = yf.Ticker("ECH").history(period="5d")
                        if len(proxy) >= 2:
                            var = ((proxy['Close'].iloc[-1] - proxy['Close'].iloc[-2]) / proxy['Close'].iloc[-2]) * 100
                            val = 6500.0 # Referencia visual estática
                    except: pass
                elif len(hist) >= 2:
                    val = float(hist['Close'].iloc[-1])
                    prev = float(hist['Close'].iloc[-2])
                    var = ((val - prev) / prev) * 100

            data.append({'Indicador': name, 'Valor': val, 'Variacion': var})

        except Exception as e:
            print(f"Error en {name}: {e}")
            data.append({'Indicador': name, 'Valor': 0.0, 'Variacion': 0.0})

    return data

def get_financial_data(tickers: list) -> tuple:
    """
    Descarga datos fundamentales y series de tiempo corregidas.
    
    Technical Debt Note:
        Yahoo Finance a veces entrega datos históricos '2y' con 1 día de retraso (lag).
        Para solucionar esto, implementamos un 'Freshness Patch' que consulta 
        '5d' y sobrescribe el último registro de cierre para garantizar tiempo real.

    Args:
        tickers (list): Lista de símbolos.

    Returns:
        tuple: (snapshot_list, history_list)
    """
    print("🏢 Extrayendo Datos Financieros (Batch Processing)...")
    snapshot = []
    history = []

    for t in tickers:
        try:
            clean_name = TICKER_MAPPING.get(t, t)

            print(f"   -> Procesando {t}...")
            stock = yf.Ticker(t)

            # 1. Historia Base (2 Años para análisis técnico)
            hist = stock.history(period="2y", auto_adjust=True)

            # Proxy Fix para IPSA local si falla la descarga principal
            if (hist.empty or len(hist) < 10) and "IPSA" in t:
                try:
                    proxy = yf.Ticker("ECH").history(period="2y", auto_adjust=True)
                    if not proxy.empty:
                        # Ajuste de escala aproximado (Factor de conversión)
                        factor = 10223.0 / proxy['Close'].iloc[-1]
                        hist = proxy * factor
                except: pass

            if hist.empty: continue

            # --- FRESHNESS PATCH (Corrección de Lag) ---
            try:
                fresh_hist = stock.history(period="5d", auto_adjust=True)
                if not fresh_hist.empty:
                    real_close = float(fresh_hist['Close'].iloc[-1])
                    long_close = float(hist['Close'].iloc[-1])
                    
                    # Si hay discrepancia significativa, forzamos el valor real
                    if abs(real_close - long_close) > 0.01:
                        hist.iloc[-1, hist.columns.get_loc('Close')] = real_close
            except Exception:
                pass # Si falla el patch, degradamos suavemente a la data histórica
            # -------------------------------------------

            # Preparación para Spark (Flattening)
            hist_reset = hist.reset_index()
            hist_reset['Date'] = hist_reset['Date'].dt.strftime('%Y-%m-%d')
            hist_reset['Ticker'] = clean_name
            hist_reset['Type'] = 'Price'
            
            # Cálculo de Medias Móviles (Technical Indicators)
            hist_reset['SMA_20'] = hist_reset['Close'].rolling(20).mean().fillna(0)
            hist_reset['SMA_50'] = hist_reset['Close'].rolling(50).mean().fillna(0)
            hist_reset['SMA_200'] = hist_reset['Close'].rolling(200).mean().fillna(0)

            # Columnas dummy para mantener esquema consistente con registros financieros
            hist_reset['Metric_Revenue'] = 0.0
            hist_reset['Metric_NetIncome'] = 0.0

            # Seleccionamos columnas finales y convertimos a dict
            hist_final = hist_reset.tail(252)[
                ['Ticker', 'Date', 'Type', 'Close', 'SMA_20', 'SMA_50', 'SMA_200', 'Metric_Revenue', 'Metric_NetIncome']
            ]
            history.extend(hist_final.to_dict('records'))

            # 2. Snapshot de Datos Fundamentales (KPIs actuales)
            info = stock.info
            price = float(hist['Close'].iloc[-1]) # Usamos el precio corregido
            p_1y = hist['Close'].iloc[-252] if len(hist) > 252 else price
            
            # Cálculo de Ratios de Liquidez (Fallback logic)
            c_liab = info.get('totalCurrentLiabilities') or info.get('currentLiabilities') or 1
            c_assets = info.get('totalCurrentAssets') or info.get('currentAssets')
            if not c_assets and info.get('currentRatio'):
                c_assets = float(info['currentRatio']) * c_liab
            elif not c_assets:
                c_assets = 0

            # Construcción del diccionario de KPIs
            snapshot.append({
                "Ticker": clean_name,
                "Precio": price,
                "Sector": info.get('sector', 'Index/Fund'),
                "Beta": float(info.get('beta', 1.0) or 1.0),
                "PE_Ratio": float(info.get('trailingPE', 0) or 0),
                "PEG_Ratio": float(info.get('trailingPegRatio', 0) or 0),
                "Quick_Ratio": float(info.get('quickRatio', 0) or 0),
                "Target_Price": float(info.get('targetMeanPrice', price) or price),
                "Consensus_Score": float(6.0 - (info.get('recommendationMean') or 3.0)), # Inversión de escala 1-5
                "Num_Analysts": int(info.get('numberOfAnalystOpinions', 0) or 0),
                "Dividend_Yield": float(info.get('dividendYield', 0) or 0),
                "52W_High": float(info.get('fiftyTwoWeekHigh', price)),
                "52W_Low": float(info.get('fiftyTwoWeekLow', price)),
                "Perf_YTD": float(((price - hist[hist.index >= f"{datetime.now().year}-01-01"].iloc[0]['Close']) / hist[hist.index >= f"{datetime.now().year}-01-01"].iloc[0]['Close'] * 100) if not hist[hist.index >= f"{datetime.now().year}-01-01"].empty else 0),
                "Perf_1Y": float(((price - p_1y)/p_1y)*100),
                "Ventas": float(info.get('totalRevenue', 0) or 0),
                "EBITDA": float(info.get('ebitda', 0) or 0),
                "Utilidad_Neta": float(info.get('netIncomeToCommon', 0) or 0),
                "Activo_Cte": float(c_assets),
                "Pasivo_Cte": float(c_liab),
                "ROE": float((info.get('returnOnEquity', 0) or 0) * 100),
                # Estimaciones simples para P&L Waterfall
                "Costo_Ventas": float((info.get('totalRevenue', 0) or 0) * 0.6),
                "OpEx": float((info.get('totalRevenue', 0) or 0) * 0.2)
            })

            # 3. Historia Financiera (Solo si no es índice)
            is_index = t.startswith('^') or 'BTC' in t or 'IPSA' in t
            if not is_index:
                fin_stmt = stock.financials
                if not fin_stmt.empty:
                    for date_idx in fin_stmt.columns[:3]: # Últimos 3 periodos
                        try:
                            rev = float(fin_stmt.loc['Total Revenue', date_idx]) if 'Total Revenue' in fin_stmt.index else 0.0
                            net = float(fin_stmt.loc['Net Income', date_idx]) if 'Net Income' in fin_stmt.index else 0.0
                            history.append({
                                'Ticker': clean_name, 'Date': date_idx.strftime('%Y-%m-%d'), 'Type': 'Financials',
                                'Close': 0.0, 'SMA_20': 0.0, 'SMA_50': 0.0, 'SMA_200': 0.0,
                                'Metric_Revenue': rev, 'Metric_NetIncome': net
                            })
                        except: pass

        except Exception as e:
            print(f"❌ Error crítico procesando {t}: {e}")

    return snapshot, history

# ==========================================
# 4. CAPA DE TRANSFORMACIÓN Y CARGA (SPARK ETL)
# ==========================================

def run_etl():
    """Función orquestadora del ETL."""
    spark = init_spark()
    api_key = get_api_key()

    # 1. Ejecución de lógica de extracción
    raw_macro = get_macro_data(api_key)
    raw_snapshot, raw_history = get_financial_data(TARGET_TICKERS)

    print("\n⚡ Iniciando Procesamiento en PySpark...")

    # --- A. PROCESAR HISTORIA (Large Dataset) ---
    # Esquema explícito para garantizar integridad de tipos
    schema_hist = StructType([
        StructField("Ticker", StringType(), False),
        StructField("Date", StringType(), False),
        StructField("Type", StringType(), False),
        StructField("Close", DoubleType(), True),
        StructField("SMA_20", DoubleType(), True),
        StructField("SMA_50", DoubleType(), True),
        StructField("SMA_200", DoubleType(), True),
        StructField("Metric_Revenue", DoubleType(), True),
        StructField("Metric_NetIncome", DoubleType(), True)
    ])

    df_hist_spark = spark.createDataFrame(raw_history, schema=schema_hist)

    # Transformación: Casting y limpieza de nulos
    df_hist_final = df_hist_spark \
        .withColumn("Close",  (col("Close").cast("double"))) \
        .fillna(0, subset=["Metric_Revenue", "Metric_NetIncome"])

    # --- B. PROCESAR KPIS (Snapshot) ---
    df_kpi_spark = spark.createDataFrame(raw_snapshot)
    
    # Transformación: Columna derivada condicional (Ejemplo de lógica Spark)
    df_kpi_final = df_kpi_spark \
        .withColumn("Estado_YTD", when(col("Perf_YTD") > 0, "Positivo").otherwise("Negativo"))

    # --- C. PROCESAR MACRO ---
    df_macro_spark = spark.createDataFrame(raw_macro)

    # --- LOAD: PUBLICACIÓN DE DATOS ESTÁTICOS ---
    print("💾 Guardando archivos CSV (Static Data Publishing)...")
    
    # Nota: Usamos toPandas().to_csv() porque Streamlit lee CSV localmente.
    # En un entorno real Big Data, esto sería df.write.parquet("s3://...")
    df_kpi_final.toPandas().to_csv("data_kpi.csv", index=False)
    df_hist_final.toPandas().to_csv("data_history.csv", index=False)
    df_macro_spark.toPandas().to_csv("data_macro.csv", index=False)

    print("✅ ETL Finalizado Correctamente.")
    spark.stop()

if __name__ == "__main__":
    run_etl()
