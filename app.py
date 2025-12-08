import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import math
from datetime import datetime

# ==========================================
# 1. CONFIGURACIÓN VISUAL
# ==========================================
st.set_page_config(page_title="Terminal Inversionista", page_icon="🏛️", layout="wide")

# PALETA DE COLORES (GLOBAL)
C_BG = "#F0F2F6"          
C_SIDEBAR = "#1E293B"     
C_TEXT = "#334155"        
C_NAVY = "#0F172A"        
C_BLUE_REV = "#2563EB"    
C_BLUE_SOFT = "rgba(37, 99, 235, 0.15)" 
C_RED_EXP = "#DC2626"     
C_RED_TEXT = "#DC2626"    
C_GREEN_TEXT = "#059669"
C_AMBER_TEXT = "#D97706"  # <--- DEFINIDA AQUÍ
C_FOREST = "#15803d"      
C_WHITE = "#FFFFFF"
C_SMA_20 = "#10B981"
C_SMA_50 = "#F59E0B"
C_SMA_200 = "#EF4444"
C_COMPARE = "#9333ea"

# COLORES GAUGE
G_RED = "#ef4444"    
G_ORANGE = "#f97316" 
G_YELLOW = "#eab308" 
G_LGREEN = "#84cc16" 
G_DGREEN = "#22c55e" 

st.markdown(f"""
<style>
    /* RESET */
    .stApp {{ background-color: {C_BG}; color: {C_TEXT}; font-family: 'Segoe UI', sans-serif; }}
    
    /* LAYOUT */
    .block-container {{ padding-top: 0rem !important;
                        padding-bottom: 2rem;
                        padding-left: 2rem;
                        padding-right: 2rem;
                        margin-top: 15px;
                          }}
    header {{ visibility: hidden; }}
    
     /* --- MAGIA CSS: UNIFICAR CONTENEDORES --- */ 
    /* Targeting containers by their fixed height attribute */
    div[data-testid="stVerticalBlock"][height="300px"], 
    div[data-testid="stVerticalBlock"][height="350px"],
    div[data-testid="stVerticalBlock"][height="500px"] {{ 
        background-color: {C_WHITE} !important; /* FONDO BLANCO */ 
        border: 1px solid #E2E8F0 !important;   /* MISMO BORDE */ 
        border-radius: 12px !important;          /* MISMAS ESQUINAS */ 
        box-shadow: 0 1px 3px rgba(0,0,0,0.02) !important; /* MISMA SOMBRA */ 
        padding: 15px !important;
    }}
    
    /* Ensure internal scroll containers are transparent so the white bg shows */
    div[data-testid="stVerticalBlock"][height="300px"] > div,
    div[data-testid="stVerticalBlock"][height="350px"] > div,
    div[data-testid="stVerticalBlock"][height="350px"] > div {{
        background-color: transparent !important;
    }}
    
    /* TICKER TAPE */
    .ticker-tape {{
        width: 100%; background-color: #FFFFFF; border-bottom: 1px solid #E2E8F0;
        padding: 8px 10px; font-family: 'Consolas', monospace; font-size: 15px;
        display: flex; gap: 20px; align-items: center; justify-content: space-between;
        box-shadow: 0 1px 2px rgba(0,0,0,0.02); border-radius: 6px; margin-bottom: 15px;
    }}
    .tape-item {{ display: flex; align-items: center; gap: 10px;}}
    .tape-lbl {{ color: #94A3B8; font-weight: 600; }}
    .tape-val {{ color: #0F172A; font-weight: 700; }}
    .t-pos {{ color: {C_GREEN_TEXT}; background-color: #ECFDF5; padding: 1px 4px; border-radius: 3px; font-size: 12px; font-weight: 600; }} 
    .t-neg {{ color: {C_RED_TEXT}; background-color: #FEF2F2; padding: 1px 4px; border-radius: 3px; font-size: 12px; font-weight: 600; }}
    
    /* KPI CARDS */
    .kpi-card {{
        background-color: {C_WHITE}; border: 1px solid #E2E8F0; border-radius: 12px;
        padding: 12px 15px; text-align: left; box-shadow: 0 1px 3px rgba(0,0,0,0.02);
        height: 100%; display: flex; flex-direction: column; justify-content: space-between;
    }}
    .kpi-title {{ font-size: 0.8rem; text-transform: uppercase; color: #64748B; font-weight: 700; margin-bottom: 2px; }}
    .kpi-val {{ font-size: 1.7rem; font-weight: 800; color: {C_SIDEBAR}; margin-bottom: 0px; line-height: 1; }}
    
    /* BADGES */
    .badge {{ padding: 2px 8px; border-radius: 8px; font-size: 0.8rem; font-weight: 700; }}
    .badge-green {{ background-color: #DCFCE7; color: #166534; }} 
    .badge-yellow {{ background-color: #FEF9C3; color: #854D0E; }}
    .badge-red {{ background-color: #FEE2E2; color: #991B1B; }} 
    .badge-blue {{ background-color: #DBEAFE; color: #1E40AF; }}
    .badge-gray {{ background-color: #F1F5F9; color: #475569; border: 1px solid #E2E8F0; }}
    
    /* HEADER & SIDEBAR */
    .header-ticker {{ font-size: 2.5rem; font-weight: 800; color: {C_SIDEBAR}; letter-spacing: -1px; }}
    section[data-testid="stSidebar"] {{ background-color: #F8FAFC; }}
    .sidebar-stat {{ margin-bottom: 8px; border-bottom: 1px solid #E2E8F0; padding-bottom: 4px; }}
    .sidebar-label {{ color: #64748B !important; font-size: 0.7rem; text-transform: uppercase; font-weight: 700; }}
    .sidebar-val {{font-weight: 800; font-size: 0.9rem; }}
    div[data-testid="stDecoration"] {{ display: none; }}
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. CARGA DE DATOS
# ==========================================
@st.cache_data
def load_data():
    try:
        kpi = pd.read_csv("data_kpi.csv")
        hist = pd.read_csv("data_history.csv")
        macro = pd.read_csv("data_macro.csv")
        
        # Fix Types
        kpi = kpi.fillna(0)
        if 'Type' not in hist.columns:
            hist['Type'] = hist.apply(lambda x: 'Price' if pd.notnull(x.get('Close')) and x.get('Close') > 0 else 'Financials', axis=1)
        
        # Fix Dates
        hist['Date'] = pd.to_datetime(hist['Date'], errors='coerce')
        
        return kpi, hist, macro
    except: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

df_kpi, df_hist, df_macro = load_data()

# ==========================================
# 3. HELPERS
# ==========================================
def format_market_cap(val):
    try: val = float(val)
    except: return "N/A"
    if val > 1e12: return f"${val/1e12:.2f} T"
    else: return f"${val/1e9:.0f} B"

def display_ticker_tape(df_m):
    if df_m.empty: return
    items = []
    order = ['USD', 'UF', 'IPSA', 'S&P500', 'NASDAQ','DOW', 'BTC', 'GOLD', 'VIX']
    for p in order:
        row = df_m[df_m['Indicador'].str.contains(p, case=False)]
        if not row.empty:
            r = row.iloc[0]
            try: val, var = float(r['Valor']), float(r['Variacion'])
            except: val, var = 0, 0
            sym, cls = ("▲", "t-pos") if var >= 0 else ("▼", "t-neg")
            fmt = f"{val:,.0f}" if p in ['UF', 'IPSA', 'BTC'] else f"{val:,.2f}"
            items.append(f"<div class='tape-item'><span class='tape-lbl'>{r['Indicador']}</span><span class='tape-val'>{fmt}</span><span class='{cls}'>{sym} {abs(var):.2f}%</span></div>")
    fecha_hoy = datetime.now().strftime("%d %b %Y").upper()
    st.markdown(f"<div class='ticker-tape'><div style='display:flex; gap:20px;'>{''.join(items)}</div><div style='margin-left:auto; color:#94A3B8; font-size:10px; font-weight:600;'>CIERRE: {fecha_hoy}</div></div>", unsafe_allow_html=True)

def kpi_card_dual(title, value, suffix="", label_text="Neutro", label_color="blue", right_content_html=""):
    html = f"<div class='kpi-card'><div style='display:flex; justify-content:space-between; align-items:center;'><div><div class='kpi-title'>{title}</div><div class='kpi-val'>{value}<span style='font-size:1.2rem; color:#94A3B8; margin-left:2px;'>{suffix}</span></div><div style='margin-top:6px;'><span class='badge badge-{label_color}'>{label_text}</span></div></div>{right_content_html}</div></div>"
    st.markdown(html, unsafe_allow_html=True)

def kpi_card_simple(title, value, suffix="", label_text="Neutro", label_color="blue"):
    html = f"<div class='kpi-card'><div class='kpi-top'><div class='kpi-title'>{title}</div><div class='kpi-val'>{value}<span style='font-size:0.6em; color:#94A3B8; margin-left:2px;'>{suffix}</span></div></div><div class='kpi-bottom' style='margin-top:6px;'><span class='badge badge-{label_color}'>{label_text}</span></div></div>"
    st.markdown(html, unsafe_allow_html=True)

def make_yoy_html(delta):
    if delta is None: return ""
    d_col = C_GREEN_TEXT if delta >= 0 else C_RED_TEXT
    d_sym = "▲" if delta >= 0 else "▼"
    return f"<div style='text-align:right;'><div style='font-size:1.2rem; font-weight:700; color:{d_col};'>{d_sym} {abs(delta):.1f}%</div><div style='font-size:0.8rem; color:#94A3B8; font-weight:500;'>vs Año Ant.</div></div>"

def make_simple_right_html(val_text, label_text, color=C_TEXT):
    return f"<div style='text-align:right;'><div style='font-size:1.2rem; font-weight:700; color:{color};'>{val_text}</div><div style='font-size:0.8rem; color:#94A3B8; font-weight:500;'>{label_text}</div></div>"

if df_kpi.empty: st.error("⚠️ Ejecute el ETL primero."); st.stop()

# ==========================================
# 4. EXTRACCIÓN DE VARIABLES GLOBALES (FIX SCOPE)
# ==========================================

# 1. Leer Sidebar Input pero NO pintar todo aun
ticker_input = st.sidebar.selectbox("Activo", df_kpi['Ticker'].unique())
# --- COMPARADOR UNIVERSAL ---
    # 1. Obtener todos los tickers disponibles en el CSV
available_tickers = sorted(df_hist['Ticker'].unique().tolist())
    
    # 2. Quitar el activo actual para no comparar consigo mismo
if ticker_input  in available_tickers: 
    available_tickers.remove(ticker_input )
    
    # 3. Crear el Selector Dinámico
compare_market = st.sidebar.selectbox("Comparar Rendimiento:", ["Sin Comparación"] + available_tickers)
    
show_smas = st.sidebar.checkbox("Ver Medias Móviles (SMA)", value=True)


# 2. Definir datos globales basados en el input
data = df_kpi[df_kpi['Ticker'] == ticker_input].iloc[0]
is_index = ticker_input.startswith('^') or 'BTC' in ticker_input

hist_price = df_hist[(df_hist['Ticker'] == ticker_input) & (df_hist['Type'] == 'Price')].sort_values('Date')
hist_fin = df_hist[(df_hist['Ticker'] == ticker_input) & (df_hist['Type'] == 'Financials')].sort_values('Date')

# 3. Extraer variables numéricas (Scope Global)
h52 = float(data.get('52W_High', 0))
l52 = float(data.get('52W_Low', 0))
perf_ytd = float(data.get('Perf_YTD', 0))
perf_1y = float(data.get('Perf_1Y', 0))

# ==========================================
# 5. RENDERIZADO UI
# ==========================================

display_ticker_tape(df_macro)

# --- SIDEBAR CONTENT ---
with st.sidebar:
    st.markdown("### 🏛️ PORTAL")
    # Inputs ya leídos arriba, solo mostramos info estática
    
    if not is_index:
        try: mkt_val = float(data['Utilidad_Neta']) * float(data['PE_Ratio'])
        except: mkt_val = 0
        mkt_cap = format_market_cap(mkt_val)
        dy_val = float(data.get('Dividend_Yield', 0)) * 100
        dy_str = f"{dy_val:.2f}%"
        dy_col = "#10B981" if dy_val > 0.1 else "#94A3B8"
    else:
        mkt_cap, dy_str, dy_col = "N/A", "--", "#94A3B8"

    # ... (El código anterior del Sector/MarketCap/Dividend se mantiene igual) ...
    st.markdown(f"<div style='margin-top:10px'></div><div class='sidebar-stat'><div class='sidebar-label'>Sector</div><div class='sidebar-val' style='font-size:0.8rem; color:#4B5563 !important'>{data.get('Sector', 'N/A')}</div></div><div class='sidebar-stat'><div class='sidebar-label'>Market Cap</div><div class='sidebar-val'>{mkt_cap}</div></div><div class='sidebar-stat' style='border:none'><div class='sidebar-label'>Dividend Yield</div><div class='sidebar-val' style='color:{dy_col} !important'>{dy_str}</div></div>", unsafe_allow_html=True)
    
    # --- LOGICA VISUAL RENDIMIENTO (Flechas y Colores) ---
    perf_ytd = float(data.get('Perf_YTD', 0))
    perf_1y = float(data.get('Perf_1Y', 0))
    
    sym_ytd, c_ytd = ("▲", "#16a34a") if perf_ytd >= 0 else ("▼", "#dc2626")
    sym_1y, c_1y = ("▲", "#16a34a") if perf_1y >= 0 else ("▼", "#dc2626")
    
    st.markdown(f"""
    <div class='sidebar-stat'>
        <div class='sidebar-label'>YTD Return</div>
        <div style='font-family:Segoe UI; font-size:0.9rem; font-weight:800; color:{c_ytd}; text-align:left;'>
            {sym_ytd} {perf_ytd:+.2f}%
        </div>
    </div>
    <div class='sidebar-stat'>
        <div class='sidebar-label'>1 Year Return</div>
        <div style='font-family:Segoe UI; font-size:0.9rem; font-weight:800; color:{c_1y}; text-align:left;'>
            {sym_1y} {perf_1y:+.2f}%
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # --- LOGICA VISUAL 52W (Distancia vs Spot) ---
    h52 = float(data.get('52W_High', 0))
    l52 = float(data.get('52W_Low', 0))
    curr_s = float(data.get('Precio', 0))
    
    # High: Siempre negativo (cuánto cayó desde el pico) -> Rojo
    d_high = ((curr_s - h52) / h52) * 100 if h52 > 0 else 0
    # Low: Siempre positivo (cuánto subió desde el piso) -> Verde
    d_low = ((curr_s - l52) / l52) * 100 if l52 > 0 else 0
    
    st.markdown("<div style='margin-top:15px; font-size:0.7rem; color:#94A3B8; font-weight:700;'>KEY STATS (vs Spot)</div>", unsafe_allow_html=True)
    st.markdown(f"""
    <div class='sidebar-stat'>
        <div class='sidebar-label'>52W High</div>
        <div style='display:flex; justify-content:space-between; align-items:center;'>
            <div class='sidebar-val'>${h52:,.2f}</div>
            <div style='color:#dc2626; font-size:0.75rem; font-weight:600;'>▼ {d_high:.1f}%</div>
        </div>
    </div>
    <div class='sidebar-stat'>
        <div class='sidebar-label'>52W Low</div>
        <div style='display:flex; justify-content:space-between; align-items:center;'>
            <div class='sidebar-val'>${l52:,.2f}</div>
            <div style='color:#16a34a; font-size:0.75rem; font-weight:600;'>▲ +{d_low:.1f}%</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
 # --- NUEVO BLOQUE: FEAR & GREED GAUGE ---
    st.markdown("<div style='margin-top:20px; font-size:0.7rem; color:#94A3B8; font-weight:700;'>SENTIMIENTO MERCADO</div>", unsafe_allow_html=True)
    
    # Buscar valor F&G en el dataframe macro
    try:
        fgi_row = df_macro[df_macro['Indicador'] == 'F&G Index']
        fgi_value = float(fgi_row['Valor'].iloc[0]) if not fgi_row.empty else 50.0
    except: fgi_value = 50.0

    # Color Dinámico F&G
    if fgi_value < 25: fgi_col, fgi_txt = "#ef4444", "EXTREME FEAR" # Rojo
    elif fgi_value < 45: fgi_col, fgi_txt = "#f97316", "FEAR"         # Naranja
    elif fgi_value < 55: fgi_col, fgi_txt = "#eab308", "NEUTRAL"      # Amarillo
    elif fgi_value < 75: fgi_col, fgi_txt = "#84cc16", "GREED"        # Verde Claro
    else: fgi_col, fgi_txt = "#22c55e", "EXTREME GREED"              # Verde Fuerte

    fig_fgi = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = fgi_value,
        number = {'font': {'size': 20, 'color': fgi_col}},
        gauge = {
            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "gray"},
            'bar': {'color': fgi_col},
            'bgcolor': "white",
            'borderwidth': 0,
            'steps': [
                {'range': [0, 25], 'color': "#fee2e2"},
                {'range': [25, 45], 'color': "#ffedd5"},
                {'range': [45, 55], 'color': "#fef9c3"},
                {'range': [55, 75], 'color': "#dcfce7"},
                {'range': [75, 100], 'color': "#d1fae5"}
            ]
        }
    ))
    fig_fgi.update_layout(
        height=130, 
        margin=dict(t=10, b=10, l=20, r=20), 
        paper_bgcolor='rgba(0,0,0,0)',
        font={'family': "Arial"}
    )
    st.plotly_chart(fig_fgi, use_container_width=True, config={'displayModeBar': False})
    st.markdown(f"<div style='text-align:center; font-weight:800; font-size:0.8rem; color:{fgi_col}; margin-top:-10px;'>{fgi_txt}</div>", unsafe_allow_html=True)


# --- HEADER ---
c_head_1, c_head_2 = st.columns([2, 1])
with c_head_1:
    st.markdown(f"<div style='display:flex; align-items:baseline; gap:8px;'><span class='header-ticker'>{ticker_input}</span><span style='color:#64748B; font-size:1.5rem; font-weight:400;'>{'Index/ETF' if is_index else 'Equity'}</span></div>", unsafe_allow_html=True)
with c_head_2:
    try:
        if not hist_price.empty:
            curr = float(hist_price.iloc[-1]['Close'])
            prev = float(hist_price.iloc[-2]['Close'])
            delta = ((curr - prev)/prev)*100
            color_hex = C_GREEN_TEXT if delta >= 0 else C_RED_TEXT
            sym = "+" if delta >= 0 else ""
            st.markdown(f"<div style='display:flex; flex-direction:column; align-items:flex-end;'><div style='display:flex; align-items:baseline; gap:10px;'><span style='font-size:2.5rem; font-weight:700; color:{C_NAVY};'>${curr:,.2f}</span><span style='color:{color_hex}; font-weight:700; font-size:1.5rem; background-color:{color_hex}15; padding:2px 6px; border-radius:4px;'>{sym}{delta:.2f}%</span></div></div>", unsafe_allow_html=True)
    except: pass

# --- KPIs ---
delta_rev, delta_net, delta_price = 0.0, 0.0, 0.0
try:
    if not is_index and len(hist_fin) >= 2:
        l_rev, p_rev = float(hist_fin.iloc[-1]['Metric_Revenue']), float(hist_fin.iloc[-2]['Metric_Revenue'])
        if p_rev != 0: delta_rev = ((l_rev - p_rev) / p_rev) * 100
        l_net, p_net = float(hist_fin.iloc[-1]['Metric_NetIncome']), float(hist_fin.iloc[-2]['Metric_NetIncome'])
        if p_net != 0: delta_net = ((l_net - p_net) / abs(p_net)) * 100
    if len(hist_price) > 252:
        p_now, p_old = float(hist_price.iloc[-1]['Close']), float(hist_price.iloc[-252]['Close'])
        delta_price = ((p_now - p_old) / p_old) * 100
except: pass

k1, k2, k3, k4, k5, k6 = st.columns(6)
if not is_index:
    roe = float(data.get('ROE', 0)); ebitda = float(data.get('EBITDA', 0)); pe = float(data.get('PE_Ratio',0)); peg = float(data.get('PEG_Ratio', 0))
    beta = float(data.get('Beta',0)); act=float(data.get('Activo_Cte',0)); pas=float(data.get('Pasivo_Cte',1)); curr_ratio = act/pas if pas>0 else 0; quick_ratio = float(data.get('Quick_Ratio', 0))
    with k1:
        lbl, col = ("Excelente", "green") if roe > 15 else ("Moderado", "yellow") if roe > 8 else ("Bajo", "red")
        kpi_card_dual("RENTABILIDAD (ROE)", f"{roe:.1f}", "%", lbl, col, make_yoy_html(delta_net))
    with k2:
        lbl, col = ("Sólido", "green") if ebitda > 0 else ("Alerta", "red")
        kpi_card_dual("EBITDA (TTM)", f"${ebitda/1e9:.1f}", "B", lbl, col, make_yoy_html(delta_rev))
    with k3:
        lbl, col = ("Subvaluada", "green") if 0 < pe < 15 else ("Premium", "yellow") if pe > 30 else ("Razonable", "blue")
        if pe <= 0: lbl, col = ("Sin Utilidades", "red")
        kpi_card_dual("VALORACIÓN (P/U)", f"{pe:.1f}", "x", lbl, col, make_yoy_html(perf_1y))
    with k4:
        lbl, col = ("Undervalued", "green") if 0 < peg < 1 else ("Overvalued", "red") if peg > 1.5 else ("Fair", "blue")
        if peg == 0: lbl, col = ("N/A", "blue"); growth_impl = 0
        else: growth_impl = (pe / peg)
        kpi_card_dual("PEG RATIO", f"{peg:.2f}", "x", lbl, col, make_simple_right_html(f"{growth_impl:.1f}%", "Growth Est.", C_SIDEBAR))
    with k5:
        lbl, col = ("Alta Vol", "yellow") if beta > 1.2 else ("Defensiva", "blue") if beta < 0.8 else ("Market", "blue")
        kpi_card_dual("RIESGO (BETA)", f"{beta:.2f}", "", lbl, col, make_simple_right_html("1.00", "Ref. S&P", "#94A3B8"))
    with k6:
        lbl, col = ("Sólida", "green") if curr_ratio > 1.2 else ("Ajustada", "red")
        qr_col = C_GREEN_TEXT if quick_ratio > 1 else C_RED_TEXT
        kpi_card_dual("LIQUIDEZ", f"{curr_ratio:.2f}", "x", lbl, col, make_simple_right_html(f"{quick_ratio:.2f}x", "Quick Ratio", qr_col))
else:
    # MODE INDEX (Usa variables globales)
    with k1: kpi_card_simple("RETORNO YTD", f"{perf_ytd:+.2f}", "%", "Positivo" if perf_ytd>0 else "Negativo", "green" if perf_ytd>0 else "red")
    with k2: kpi_card_simple("RETORNO 1 AÑO", f"{perf_1y:+.2f}", "%", "Anual", "green" if perf_1y>0 else "red")
    with k3: 
        curr_p = float(data.get('Precio', 0))
        dist = ((curr_p - h52)/h52)*100 if h52 else 0
        kpi_card_simple("DIST. MÁXIMO", f"{dist:.1f}", "%", f"High: {h52:,.0f}", "blue")
    with k4: kpi_card_simple("VOLATILIDAD", "N/A", "", "Standard", "blue")

st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

# --- ROW 2: MAIN CHART ---
c_chart_main = st.container()
with c_chart_main:
    with st.container(border=True, height=350):
        hist_price['Date'] = pd.to_datetime(hist_price['Date'])
        fig = go.Figure()
        
        if compare_market != "Sin Comparación":
            # El valor del selector ES el ticker (ej: "MSFT", "^IPSA")
            bench_ticker = compare_market
            
            # Buscamos datos del comparativo
            hist_bench = df_hist[df_hist['Ticker'] == bench_ticker].sort_values('Date').drop_duplicates(subset=['Date'], keep='last')
            
            if not hist_bench.empty:
                # Normalizar fechas para Merge
                hist_bench['Date'] = pd.to_datetime(hist_bench['Date']).dt.tz_localize(None)
                hist_price['Date'] = hist_price['Date'].dt.tz_localize(None)
                
                merged = pd.merge(hist_price, hist_bench, on='Date', suffixes=('', '_b'), how='inner')
                
                if not merged.empty:
                    # Cálculo Base 0%
                    base_stock = merged['Close'].iloc[0]
                    base_bench = merged['Close_b'].iloc[0]
                    
                    merged['Pct_Stock'] = ((merged['Close'] - base_stock) / base_stock) * 100
                    merged['Pct_Bench'] = ((merged['Close_b'] - base_bench) / base_bench) * 100
                    
                    fig.add_trace(go.Scatter(x=merged['Date'], y=merged['Pct_Stock'], mode='lines', name=ticker_input, line=dict(color=C_BLUE_REV, width=3), fill='tozeroy', fillcolor=C_BLUE_SOFT))
                    fig.add_trace(go.Scatter(x=merged['Date'], y=merged['Pct_Bench'], mode='lines', name=bench_ticker, line=dict(color=C_COMPARE, width=1.5, dash='solid')))
                    y_tit, tick_suf = "Rendimiento (%)", "%"
                else:
                     y_tit, tick_suf = "Precio ($)", ""
            else: 
                y_tit, tick_suf = "Precio ($)", ""
                st.warning(f"No hay datos históricos coincidentes para {bench_ticker}")
                fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['Close'], mode='lines', name=ticker_input, line=dict(color=C_BLUE_REV, width=3), fill='tozeroy', fillcolor=C_BLUE_SOFT))
        else:
            # MODO PRECIO (SIN CAMBIOS)
            fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['Close'], mode='lines', name=ticker_input, line=dict(color=C_BLUE_REV, width=3), fill='tozeroy', fillcolor=C_BLUE_SOFT))
            if show_smas:
                if 'SMA_20' in hist_price.columns: fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['SMA_20'], mode='lines', name='SMA 20', line=dict(color=C_SMA_20, width=1)))
                if 'SMA_50' in hist_price.columns: fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['SMA_50'], mode='lines', name='SMA 50', line=dict(color=C_SMA_50, width=1)))
                if 'SMA_200' in hist_price.columns: fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['SMA_200'], mode='lines', name='SMA 200', line=dict(color=C_SMA_200, width=1)))
            y_tit, tick_suf = "Precio ($)", ""

        if fig.data:
            y_vals = fig.data[0].y
            min_y, max_y = min(y_vals)*0.98, max(y_vals)*1.02
        else: min_y, max_y = 0, 100

        fig.update_layout(
            title=dict(text=f"<b>EVOLUCIÓN ({'COMPARATIVA' if 'Sin' not in compare_market else 'PRECIO'})</b>", font=dict(size=11, color=C_SIDEBAR), x=0.01, y=0.96),
            paper_bgcolor='white', plot_bgcolor='white', height=320,
            margin=dict(t=30, b=10, l=10, r=10),
            xaxis=dict(showgrid=False, linecolor='#E2E8F0', tickfont=dict(color='#64748B', size=9), tickformat="%b %Y"),
            yaxis=dict(range=[min_y, max_y], showgrid=True, gridcolor='#F1F5F9', gridwidth=1, tickfont=dict(color='#64748B', size=9), title=None, ticksuffix=tick_suf),
            hovermode="x unified", showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        

def interpolate(val, x0, y0, x1, y1):
    return y0 + (val - x0) * (y1 - y0) / (x1 - x0)

def calculate_needle_angle(score):
    """
    Calculates the needle angle using piecewise interpolation.
    1 -> 200 (Matches user preference)
    2 -> 135 (Matches standard gauge tick)
    3 -> 90  (Matches standard gauge tick)
    4 -> 45  (Matches standard gauge tick)
    5 -> -20 (Matches user preference)
    """
    safe_score = max(1.0, min(5.0, float(score)))
    
    if safe_score <= 2.0:
        return interpolate(safe_score, 1.0, 200, 2.0, 130)
    elif safe_score <= 3.0:
        return interpolate(safe_score, 2.0, 135, 3.0, 90)
    elif safe_score <= 4.0:
        return interpolate(safe_score, 3.0, 90, 4.0, 50)
    else:
        return interpolate(safe_score, 4.0, 45, 5.0, -20)

# --- ROW 3: BOTTOM ---
if not is_index:
   # st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    c1, c2 = st.columns([1, 2])
    with c1:
       with st.container(border=True, height=350): # Se pintará de blanco por CSS  
        st.markdown(f"<div style='font-size:1.3rem; font-weight:800; color:{C_SIDEBAR}; margin-bottom:15px;'>CONSENSO DE ANALISTAS</div>", unsafe_allow_html=True)  
          
        # DATOS  
        curr_p = data['Precio']  
        target_p = data['Target_Price']  
        num_analysts = int(data.get('Num_Analysts', 0))  
        score = data.get('Consensus_Score', 3.0)   
          
        # MATEMÁTICA AGUJA  
        angle = calculate_needle_angle(score)
          
        center_x, center_y = 0.5, 0.2  
        radius = 0.35   
        rad = math.radians(angle)  
        x_head = center_x + radius * math.cos(rad)  
        y_head = center_y + radius * math.sin(rad)  
 
        # Tapered needle base 
        base_len = 0.04 
        x_b1 = center_x + base_len * math.cos(rad + math.pi/2) 
        y_b1 = center_y + base_len * math.sin(rad + math.pi/2) 
        x_b2 = center_x + base_len * math.cos(rad - math.pi/2) 
        y_b2 = center_y + base_len * math.sin(rad - math.pi/2) 
  
        # TEXTO RECOMENDACIÓN  
        if score >= 4.2: rec_txt, rec_col = "COMPRA FUERTE", G_DGREEN  
        elif score >= 3.4: rec_txt, rec_col = "COMPRAR", G_LGREEN  
        elif score >= 2.6: rec_txt, rec_col = "MANTENER", G_YELLOW  
        elif score >= 1.8: rec_txt, rec_col = "VENDER", G_ORANGE  
        else: rec_txt, rec_col = "VENTA FUERTE", G_RED  
  
        upside_pct = ((target_p - curr_p) / curr_p) * 100  
        up_sym = "▲" if upside_pct > 0 else "▼"  
        up_col = C_GREEN_TEXT if upside_pct > 0 else C_RED_TEXT   
          
        # Ajuste de columnas: 25% texto, 75% gauge 
        col_data, col_viz = st.columns([9.5, 20])  
          
        with col_data:  
            st.markdown(f"""  
            <div style='height: 10px;'></div>          
            </br>                         
            <div style='line-height: 1.1;'>  
                <div style='font-size: 0.9rem; color: #64748B; font-weight: 600; margin-bottom:4px;'>OBJETIVO (12M)</div>  
                <div style='font-size: 2.0rem; font-weight: 800; color: {C_NAVY}; margin-bottom: 5px;'>${target_p:.2f}</div>  
                <span style='background-color: {up_col}15; color: {up_col}; padding: 3px 8px; border-radius: 6px;margin-top: 10px; font-weight: 700; font-size: 1.1rem;'>  
                    {up_sym} {upside_pct:.1f}% Potencial  
                </span>  
                <div style='color: #94A3B8; font-size: 1rem; margin-top: 15px;'>Actual: ${curr_p:.2f}</div>  
                <div style='color: #94A3B8; font-size: 1rem; margin-top: 2px;'>Base: {num_analysts} Analistas.</div>  
            </div>  
            """, unsafe_allow_html=True)  
              
        with col_viz: 
            # 1. CLAMPING DEL SCORE (Asegurar que esté entre 1 y 5 para no romper el ángulo) 
            safe_score = max(1.0, min(5.0, float(score))) 
 
            # 2. CÁLCULO TRIGONOMÉTRICO DE LA AGUJA 
            # Rango: 1 (Izquierda/180°) -> 5 (Derecha/0°) 
            # Porcentaje del recorrido (0.0 a 1.0) 
            # pct = (safe_score - 1) / 4.0  
             
            # Ángulo en Grados y Radianes 
            # angle = 180 - (pct * 180) 
            angle = calculate_needle_angle(safe_score)
            rad = math.radians(angle) 
             
            # Geometría del Centro y Radio 
            # cy = 0.25 coloca el centro un poco más arriba para alinear con los números 
            cx, cy = 0.5, 0.20  
            r_head = 0.35  # Largo de la punta (Radio) 
            r_base = 0.03  # Ancho de la base de la aguja 
 
            # Coordenadas de la Punta (Head) 
            x_head = cx + r_head * math.cos(rad) 
            y_head = cy + r_head * math.sin(rad) 
 
            # Coordenadas de la Base (Izquierda y Derecha de la aguja, perpendiculares al ángulo) 
            # Ángulo base 1 = ángulo + 90 grados 
            x_b1 = cx + r_base * math.cos(rad + math.pi/2) 
            y_b1 = cy + r_base * math.sin(rad + math.pi/2) 
             
            # Ángulo base 2 = ángulo - 90 grados 
            x_b2 = cx + r_base * math.cos(rad - math.pi/2) 
            y_b2 = cy + r_base * math.sin(rad - math.pi/2) 
 
            # Construcción del "Path" SVG para el triángulo 
            path_needle = f"M {x_head} {y_head} L {x_b1} {y_b1} L {x_b2} {y_b2} Z" 
 
            # 3. CREAR GRÁFICO 
            fig_gauge = go.Figure(go.Indicator( 
                mode = "gauge", 
                value = safe_score, 
                gauge = { 
                    'axis': { 
                        'range': [1, 5],  
                        'tickvals': [1, 2, 3, 4, 5],  
                        'ticktext': ['1', '2', '3', '4', '5'],  
                        'tickwidth': 1,  
                        'tickcolor': "#333" 
                    }, 
                    'bar': {'color': "rgba(0,0,0,0)"}, # Barra invisible (usamos nuestra aguja) 
                    'bgcolor': "white", 
                    'borderwidth': 0, 
                    'steps': [ 
                        {'range': [1, 1.8], 'color': G_RED}, 
                        {'range': [1.8, 2.6], 'color': G_ORANGE}, 
                        {'range': [2.6, 3.4], 'color': G_YELLOW}, 
                        {'range': [3.4, 4.2], 'color': G_LGREEN}, 
                        {'range': [4.2, 5], 'color': G_DGREEN} 
                    ] 
                } 
            )) 
             
            # DIBUJAR LA AGUJA MANUALMENTE 
            fig_gauge.add_shape(type="path", path=path_needle, fillcolor=C_NAVY, line_color=C_NAVY) 
             
            # DIBUJAR EL PUNTO CENTRAL (Tapa la base de la aguja) 
            fig_gauge.add_shape(type="circle", x0=cx-0.03, y0=cy-0.03, x1=cx+0.03, y1=cy+0.03, fillcolor=C_NAVY, line_color=C_NAVY) 
             
            # ANOTACIÓN DE TEXTO ("COMPRAR", "VENDER") 
            fig_gauge.add_annotation(x=0.5, y=-0.05, text=f"<b>{rec_txt}</b>", showarrow=False, font=dict(size=25, color=rec_col))
 
            # AJUSTE DE MÁRGENES PARA QUE NO SE CORTE 
            fig_gauge.update_layout( 
                height=190, # Un poco más alto para que quepa todo 
                margin=dict(t=20, b=5, l=10, r=10),  
                paper_bgcolor='rgba(0,0,0,0)',  
                plot_bgcolor='rgba(0,0,0,0)' 
            ) 
             
            st.plotly_chart(fig_gauge, use_container_width=True, config={'displayModeBar': False})

            # --- EL SCORE ESTÁ AQUÍ (FUERA DEL GRÁFICO, HTML PURO) ---
            st.markdown(f"""
            <div style='text-align: center; margin-top: -5px;'>
                <div style='font-size:1.5rem; font-weight:bold; color:#475569;'>Calificación: <span style='color:{C_SIDEBAR}'>{score:.1f}</span></div>
             </div>
            """, unsafe_allow_html=True)
    with c2:
        with st.container(border=True, height=350): # Se pintará de blanco por CSS 
            st.markdown(f"<div style='font-size:0.7rem; font-weight:700; color:{C_SIDEBAR}; margin-bottom:15px;'>CASCADA P&L (12M)</div>", unsafe_allow_html=True)
            rev = data['Ventas'] 
            cogs = -abs(data['Costo_Ventas']) 
            gross = rev + cogs 
            opex_real = -abs(data['OpEx']) if data['OpEx'] != 0 else -(gross - data['Utilidad_Neta']) 
            net = data['Utilidad_Neta'] 
         
            fig_water = go.Figure(go.Waterfall( 
                measure = ["relative", "relative", "total", "relative", "total"], 
                x = ["Ventas", "Costos", "Bruta", "Gastos", "Neta"], 
                y = [rev, cogs, 0, opex_real, 0], 
                connector = {"line":{"color":"#94A3B8", "width":1}}, 
                decreasing = {"marker":{"color": C_RED_EXP}},  
                increasing = {"marker":{"color": C_BLUE_REV}},  
                totals = {"marker":{"color": C_FOREST}}  
            )) 
         
            fig_water.update_layout( 
                # Height adjusted to fit within FIXED_HEIGHT along with title
                height=280, margin=dict(t=0, b=10, l=0, r=0),  
                paper_bgcolor='rgba(0,0,0,0)', # TRANSPARENTE PARA USAR FONDO BLANCO DEL CONTAINER 
                plot_bgcolor='rgba(0,0,0,0)', 
                yaxis=dict(title=None, showgrid=False, showline=True, linecolor='#E2E8F0', tickfont=dict(size=9)),  
                xaxis=dict(showline=True, linecolor='#E2E8F0', tickfont=dict(size=10)) 
            ) 
            st.plotly_chart(fig_water, use_container_width=True, config={'displayModeBar': False}) 

st.markdown("<br><div style='text-align: center; color: #94A3B8; font-size: 10px;'>Market Guardian v61.0 (Fix Scope & Variables) | Datos Diferidos</div>", unsafe_allow_html=True)