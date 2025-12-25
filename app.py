import streamlit as st 
import pandas as pd 
import plotly.graph_objects as go 
import math 
from datetime import datetime, timedelta 
 
# ========================================== 
# 1. CONFIGURACIÓN VISUAL 
# ========================================== 
st.set_page_config(page_title="Terminal Inversionista", page_icon="🏛️", layout="wide") 

# --- DICCIONARIOS DE TEMA ---
THEME_LIGHT = {
    "C_BG": "#F0F2F6",
    "C_SIDEBAR_BG": "#F8FAFC",
    "C_SIDEBAR_TEXT": "#334155", # C_TEXT original
    "C_SIDEBAR_TITLE": "#0F172A",
    "C_CARD_BG": "#FFFFFF",
    "C_CARD_BORDER": "#E2E8F0",
    "C_TEXT_MAIN": "#334155",
    "C_TEXT_SEC": "#64748B",
    "C_NAVY": "#0F172A", 
    "C_BLUE_REV": "#2563EB",
    "C_BLUE_SOFT": "rgba(37, 99, 235, 0.15)",
    "C_CHART_GRID": "#F1F5F9",
    "C_CHART_TEXT": "#334155",
    "C_TICKER_BG": "#FFFFFF",
    "BOX_SHADOW": "0 1px 3px rgba(0,0,0,0.02)",
    "EXPANDER_BG": "#FFFFFF",
    "EXPANDER_HEADER_BG": "#F1F5F9",
    "HOVER_BG": "#F1F5F9",
}

THEME_DARK = {
    "C_BG": "#0E1117", # Bloomberg Dark
    "C_SIDEBAR_BG": "#1E1E1E", # Sidebar diferenciado
    "C_SIDEBAR_TEXT": "#E0E0E0",
    "C_SIDEBAR_TITLE": "#FAFAFA",
    "C_CARD_BG": "#262730", # Tarjetas gris medio
    "C_CARD_BORDER": "#3b3c46", # Borde sutil
    "C_TEXT_MAIN": "#FAFAFA", # Blanco hueso
    "C_TEXT_SEC": "#9CA3AF", # Gris claro
    "C_NAVY": "#E0E0E0", 
    "C_BLUE_REV": "#3B82F6", # Azul Neón/Cielo
    "C_BLUE_SOFT": "rgba(59, 130, 246, 0.2)",
    "C_CHART_GRID": "#333333",
    "C_CHART_TEXT": "#E0E0E0",
    "C_TICKER_BG": "#1E1E1E",
    "BOX_SHADOW": "none", 
    "EXPANDER_BG": "#262730",
    "EXPANDER_HEADER_BG": "#3b3c46",
    "HOVER_BG": "#3b3c46",
}

# --- GESTIÓN DE ESTADO DEL TEMA ---
# 1. Recuperar TEMA desde URL si existe (Persistencia Crítica antes de definir constantes)
qp = st.query_params
if 'theme' in qp:
    st.session_state['theme'] = qp['theme']

if 'theme' not in st.session_state:
    st.session_state['theme'] = 'Light'

current_theme = THEME_LIGHT if st.session_state['theme'] == 'Light' else THEME_DARK

# Mapeo de variables dinámicas
C_BG = current_theme["C_BG"]
C_SIDEBAR_BG = current_theme["C_SIDEBAR_BG"]
C_SIDEBAR_TEXT = current_theme["C_SIDEBAR_TEXT"] # Used for values in sidebar
C_SIDEBAR_TITLE = current_theme["C_SIDEBAR_TITLE"]
C_CARD_BG = current_theme["C_CARD_BG"]
C_CARD_BORDER = current_theme["C_CARD_BORDER"]
C_TEXT = current_theme["C_TEXT_MAIN"] 
C_TEXT_SEC = current_theme["C_TEXT_SEC"]
C_NAVY = current_theme["C_NAVY"]
C_SIDEBAR = C_NAVY # Alias para compatibilidad
C_BLUE_REV = current_theme["C_BLUE_REV"]
C_BLUE_SOFT = current_theme["C_BLUE_SOFT"]
C_CHART_GRID = current_theme["C_CHART_GRID"]
C_CHART_TEXT = current_theme["C_CHART_TEXT"]
C_TICKER_BG = current_theme["C_TICKER_BG"]
BOX_SHADOW = current_theme["BOX_SHADOW"]
EXPANDER_BG = current_theme["EXPANDER_BG"]
EXPANDER_HEADER_BG = current_theme["EXPANDER_HEADER_BG"]
HOVER_BG = current_theme["HOVER_BG"]
C_WHITE = C_CARD_BG # Remapping white to card bg for dynamic logic

# Colores estáticos
C_RED_EXP = "#DC2626"
C_RED_TEXT = "#DC2626"
C_GREEN_TEXT = "#059669"
C_AMBER_TEXT = "#D97706"
C_FOREST = "#15803d"
C_SMA_20 = "#10B981"
C_SMA_50 = "#F59E0B"
C_SMA_200 = "#EF4444"
C_COMPARE = "#9333ea"
 
# COLORES GAUGE 
G_RED = "#ef4444" 
G_ORANGE = "#f97316" 
G_YELLOW = "#FFD700" 
G_LGREEN = "#84cc16" 
G_DGREEN = "#22c55e" 
 
# === CSS ORIGINAL RESTAURADO (SIN MODIFICACIONES DE FUENTE) ===
st.markdown(f""" 
<style> 
    /* ========================================= 
       1. RESET & LAYOUT 
       ========================================= */ 
    .stApp {{ background-color: {C_BG}; color: {C_TEXT}; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }} 
     
    [data-testid="stHeader"] {{ display: none !important; }} 
    [data-testid="stToolbar"] {{ display: none !important; }} 
    [data-testid="stDecoration"] {{ display: none !important; }} 
    footer {{ display: none !important; }} 
 
    .block-container {{ 
        padding-top: 0rem !important; 
        padding-bottom: 1rem !important; 
        padding-left: 2rem !important; 
        padding-right: 2rem !important; 
        max-width: 100% !important; 
    }} 
     
    html {{ font-size: 14px; }} 
     
    /* ========================================= 
       2. TARJETAS & COMPONENTES 
       ========================================= */ 
    div[data-testid="stVerticalBlock"][height="300px"],  
    div[data-testid="stVerticalBlock"][height="330px"],  
    div[data-testid="stVerticalBlock"][height="350px"], 
    div[data-testid="stVerticalBlock"][height="500px"] {{  
        background-color: {C_CARD_BG} !important;  
        border: 1px solid {C_CARD_BORDER} !important;    
        border-radius: 12px !important;           
        box-shadow: {BOX_SHADOW} !important;  
        padding: 15px !important; 
        gap: 0px !important; 
    }} 
     
    div[data-testid="stVerticalBlock"][height="300px"] > div, 
    div[data-testid="stVerticalBlock"][height="330px"] > div,  
    div[data-testid="stVerticalBlock"][height="350px"] > div, 
    div[data-testid="stVerticalBlock"][height="500px"] > div {{ 
        background-color: transparent !important; 
    }} 
 
    /* KPI CARDS */ 
    .kpi-card {{ 
        background-color: {C_CARD_BG}; border: 1px solid {C_CARD_BORDER}; border-radius: 12px; 
        padding: 10px 12px; 
        text-align: left; box-shadow: {BOX_SHADOW}; 
        height: 100%; display: flex; flex-direction: column; justify-content: space-between; 
    }} 
    .kpi-title {{ font-size: 0.75rem; text-transform: uppercase; color: {C_TEXT_SEC}; font-weight: 700; margin-bottom: 2px; }} 
    .kpi-val {{ font-size: 1.5rem; font-weight: 800; color: {C_NAVY}; margin-bottom: 0px; line-height: 1; }} 
    .badge {{ padding: 2px 6px; border-radius: 6px; font-size: 0.75rem; font-weight: 700; }} 
    .badge-green {{ background-color: #DCFCE7; color: #166534; }}  
    .badge-yellow {{ background-color: #FEF9C3; color: #854D0E; }} 
    .badge-red {{ background-color: #FEE2E2; color: #991B1B; }}  
    .badge-blue {{ background-color: #DBEAFE; color: #1E40AF; }} 
 
    /* HEADER & SIDEBAR */ 
    .header-ticker {{   
        font-size: 3.5rem !important;
        font-weight: 900;   
        color: {C_SIDEBAR_TITLE};
        letter-spacing: -2px;
        line-height: 1;
    }}  
  
     section[data-testid="stSidebar"] {{  
        background-color: {C_SIDEBAR_BG};   
    }}  
    div[data-testid="stSidebarHeader"] {{  
        display: none !important;  
    }}  
  
    section[data-testid="stSidebar"] .block-container {{  
        padding-top: 2rem !important;
        padding-bottom: 2rem !important;  
    }}  
      
    section[data-testid="stSidebar"] h3 {{  
        margin-top: 1rem !important;  
        margin-bottom: 1rem !important;  
        font-size: 1.4rem !important;
        font-weight: 900 !important;
        letter-spacing: -1px;   
        color: {C_SIDEBAR_TITLE} !important;
    }}  
  
    .sidebar-stat {{ margin-bottom: 8px; border-bottom: 1px solid {C_CARD_BORDER}; padding-bottom: 4px;font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif !important; }}  
    .sidebar-label{{ color: {C_TEXT_SEC} !important; font-size: 0.8rem; text-transform: uppercase; font-weight: 700;font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif !important; }}  
    .sidebar-val {{ font-weight: 800; font-size: 0.9rem; color: {C_SIDEBAR_TEXT} !important; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif !important; }}  
 
    /* === COMPACTAR SIDEBAR === */ 
    section[data-testid="stSidebar"] .stElementContainer {{ 
        margin-bottom: 0.3rem !important;
    }} 
    section[data-testid="stSidebar"] h3 {{ 
        margin-bottom: 0.2rem !important; 
    }} 
    .sidebar-sep {{ 
        border-top: 1px solid {C_CARD_BORDER}; 
        margin-top: 8px; 
        margin-bottom: 8px; 
    }} 
    .sidebar-header {{
        font-size: 0.83rem !important;
        font-weight: 700 !important;
        color: {C_SIDEBAR_TITLE} !important;
        text-transform: uppercase !important;
        margin-bottom: 10px !important;
        font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif !important;
    }}

    .sidebar-percent {{
        font-size: 12px !important;
        font-weight: 700 !important;
        text-align: right !important;
        padding: 2px 6px !important;
        border-radius: 4px !important;
        white-space: nowrap !important;
        display: inline-block !important;
        min-width: 50px !important;
        font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif !important;
    }}
 
    /* === WATCHLIST STYLES === */ 
    section[data-testid="stSidebar"] .stButton button {{ 
        min-height: 35px !important;     
        height: 35px !important; 
        width: 100% !important; 
        background-color: {C_CARD_BG} !important; 
        border: 1px solid {C_CARD_BORDER} !important; 
        border-radius: 8px !important; 
        color: {C_TEXT} !important;
        padding: 0px !important; 
        margin: 0px !important; 
        transform: translateY(1px) !important;  
        position: relative !important; 
        box-shadow: {BOX_SHADOW} !important; 
        transition: all 0.2s ease !important; 
        overflow: hidden !important;  
    }} 
    section[data-testid="stSidebar"] .stButton button div[data-testid="stMarkdownContainer"] {{ 
        opacity: 0 !important; 
    }} 
    section[data-testid="stSidebar"] .stButton button::after {{ 
        content: "+" !important; 
        position: absolute !important; 
        top: 50% !important; 
        left: 50% !important; 
        transform: translate(-52%, -64%) !important; 
        font-family: "Segoe UI", sans-serif !important; 
        font-size: 34px !important;
        font-weight: 500 !important; 
        color: {C_SIDEBAR_TEXT} !important; 
        line-height: 1 !important; 
    }} 
    section[data-testid="stSidebar"] .stButton button:hover {{ 
        border-color: #ff4b4b !important; 
        background-color: {HOVER_BG} !important; 
        box-shadow: 0 2px 4px rgba(255, 75, 75, 0.15) !important; 
    }} 
    section[data-testid="stSidebar"] .stButton button:hover::after {{ 
        color: #ff4b4b !important; 
    }} 
     
    /* PROTECCIÓN EXPANDER */ 
    div[data-testid="stExpander"] .stButton button {{ 
        min-height: 0px !important; 
        height: auto !important; 
        border: none !important; 
        background-color: transparent !important; 
        box-shadow: none !important; 
        transform: none !important;  
    }} 
    div[data-testid="stExpander"] .stButton button div[data-testid="stMarkdownContainer"] {{ 
        opacity: 1 !important; 
    }} 
    div[data-testid="stExpander"] .stButton button::after {{ 
        content: none !important; 
    }} 
    div[data-testid="stExpander"] div[data-testid="stVerticalBlock"] {{ 
        gap: 0.5rem !important; 
    }} 
    div[data-testid="stExpander"] div[data-testid="element-container"] {{ 
        margin-bottom: 0px !important; 
    }} 
 
    /* CONTENEDOR WATCHLIST */ 
    .wl-container {{ 
        background-color: transparent; 
        border-radius: 0px; 
        padding: 0px 0px; 
        margin-top: 10px; 
        font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; 
        margin-left: -2rem !important; 
        margin-right: -3rem !important;   
        width: calc(100% + 5rem) !important;   
    }} 
    .wl-title {{ 
        padding-left: 2rem !important;  
        padding-right: 4rem !important; 
    }} 
    .wl-header {{ 
        color: {C_TEXT_SEC}; 
        font-size: 11px; 
        padding: 0px 2rem 6px 2rem !important;  
        border-bottom: 1px solid {C_CARD_BORDER}; 
        margin-bottom: 4px; 
        font-weight: 700; 
        display: grid;  
        grid-template-columns: 22% 33% 45%; 
        white-space: nowrap; 
    }} 
    .wl-row {{ 
        display: grid; 
        grid-template-columns: 22% 33% 45%; 
        align-items: center; 
        padding: 4px 2.1rem !important; 
        cursor: pointer; 
        text-decoration: none !important; 
        transition: background-color 0.15s ease; 
        border-radius: 0px !important; 
    }} 
    .wl-row:hover {{ 
        background-color: {HOVER_BG}; 
    }} 
    .wl-sym {{ 
        font-size: 13px; 
        font-weight: 700; 
        color: {C_SIDEBAR_TITLE}; 
        white-space: nowrap; 
        overflow: hidden; 
        text-overflow: ellipsis; 
        text-align: left; 
    }} 
    .wl-price {{ 
        font-size: 13px; 
        color: {C_SIDEBAR_TEXT}; 
        text-align: right; 
        font-variant-numeric: tabular-nums;  
        padding-right: 5px;  
        overflow: hidden; 
        white-space: nowrap; 
    }} 
    .wl-col-end {{ 
        display: flex;  
        justify-content: flex-end;  
        align-items: center; 
        gap: 8px; 
    }} 
    .wl-up {{ color: #059669; background-color: #ECFDF5;text-align: right }}  
    .wl-down {{ color: #DC2626; background-color: #FEF2F2;text-align: right }} 
     
    .wl-col-end object {{ 
        display: inline-block; 
        width: 16px; 
        height: 16px; 
        vertical-align: middle; 
        pointer-events: auto; 
    }} 
    .wl-del {{ 
        color: #94A3B8; 
        font-size: 14px; 
        font-weight: bold; 
        text-decoration: none; 
        display: flex; 
        align-items: center; 
        justify-content: center; 
        width: 100%; 
        height: 100%; 
        visibility: hidden; 
    }} 
    .wl-row:hover .wl-del {{ 
        visibility: visible; 
    }} 
    .wl-del:hover {{ 
        color: #EF4444;  
    }} 

    /* EXPANDER STYLING */
    section[data-testid="stSidebar"] details {{
        border: 1px solid {C_CARD_BORDER} !important;
        border-radius: 8px !important;
        background-color: {EXPANDER_BG} !important;
        margin-bottom: 10px !important;
    }}
    section[data-testid="stSidebar"] details summary {{
        background-color: {EXPANDER_HEADER_BG} !important;
        padding: 10px 8px !important;
        font-weight: 700 !important;
        font-size: 0.75rem !important; 
        color: {C_SIDEBAR_TEXT} !important;
        border-radius: 8px !important;
        cursor: pointer !important;
        transition: background-color 0.2s ease !important;
        list-style: none !important; 
    }}
    section[data-testid="stSidebar"] details[open] summary {{
        border-bottom: 1px solid {C_CARD_BORDER} !important;
        border-bottom-left-radius: 0 !important;
        border-bottom-right-radius: 0 !important;
        background-color: {EXPANDER_HEADER_BG} !important;
    }}
    section[data-testid="stSidebar"] details summary:hover {{
        background-color: {EXPANDER_HEADER_BG} !important;
    }}
      
</style>  
""", unsafe_allow_html=True)  
 
# ========================================== 
# 2. CARGA DE DATOS ROBUSTA 
# ========================================== 
@st.cache_data 
def load_data(): 
    try: 
        kpi = pd.read_csv("data_kpi.csv") 
        hist = pd.read_csv("data_history.csv") 
        macro = pd.read_csv("data_macro.csv") 
        kpi = kpi.fillna(0) 
        hist['Date'] = pd.to_datetime(hist['Date'], errors='coerce') 
        return kpi, hist, macro 
    except FileNotFoundError: 
        st.error("⚠️ Archivos de datos no encontrados. Ejecute 'src/etl_spark.py' primero.") 
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() 
    except Exception as e: 
        st.error(f"⚠️ Error cargando datos: {e}") 
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() 
 
df_kpi, df_hist, df_macro = load_data() 
 
# ========================================== 
# 3. HELPERS VISUALES 
# ========================================== 
def get_watchlist_data(tickers, df_hist): 
    data = [] 
    unique_tickers = list(dict.fromkeys(tickers)) 
    for t in unique_tickers: 
        h = df_hist[(df_hist['Ticker'] == t) & (df_hist['Type'] == 'Price')].sort_values('Date') 
        if h.empty: h = df_hist[df_hist['Ticker'] == t].sort_values('Date') 
         
        if not h.empty: 
            last_close = float(h.iloc[-1]['Close']) 
            if len(h) > 1: 
                prev_close = float(h.iloc[-2]['Close']) 
                chg = ((last_close - prev_close)/prev_close)*100 if prev_close != 0 else 0 
            else: chg = 0 
        else: 
            last_close = 0.0 
            chg = 0.0 
        data.append({'Symbol': t, 'Last': last_close, 'Chg%': chg}) 
    return pd.DataFrame(data) 
 
def format_market_cap(val): 
    try: val = float(val) 
    except: return "N/A" 
    if val > 1e12: return f"${val/1e12:.2f} T" 
    else: return f"${val/1e9:.0f} B" 
 
def display_ticker_tape(df_m): 
    if df_m.empty: return None 
    items_html = [] 
    order = ['USD', 'EUR', 'UF', 'IPSA', 'S&P500', 'NASDAQ', 'DOW', 'BTC', 'COBRE', 'ORO', 'VIX'] 
    for p in order: 
        row = df_m[df_m['Indicador'].str.upper() == p] 
        if row.empty: row = df_m[df_m['Indicador'].str.contains(p, case=False, na=False)] 
        if not row.empty: 
            r = row.iloc[0] 
            try:  
                val, var = float(r['Valor']), float(r['Variacion']) 
            except: val, var = 0, 0 
            sym, cls = ("▲", "t-pos") if var >= 0 else ("▼", "t-neg") 
            fmt = f"{val:,.0f}" if p in ['UF', 'IPSA', 'BTC'] else f"{val:,.2f}" 
            item = f"<div class='ticker-item'><span class='tape-lbl'>{r['Indicador']}</span><span class='tape-val'>{fmt}</span><span class='{cls}'>{sym} {abs(var):.2f}%</span></div>" 
            items_html.append(item) 
             
    fecha_txt = (datetime.now() - timedelta(days=1)).strftime("%d %b").upper() 
    items_html.append(f"<div class='ticker-item' style='border-right:none;'><span style='color:{C_TEXT_SEC}; font-size:14px; font-weight:700;'>📅 CIERRE: {fecha_txt}</span></div>") 
    full_content = "".join(items_html) * 15 
 
    css_animation = f""" 
    <style> 
    .ticker-wrap {{ width: 100%; overflow: hidden; background-color: {C_TICKER_BG}; border-bottom: 1px solid {C_CARD_BORDER}; padding: 12px 0; white-space: nowrap; box-shadow: {BOX_SHADOW}; margin-bottom: 15px; }} 
    .ticker-move {{ display: inline-block; animation: ticker-scroll 120s linear infinite; }} 
    .ticker-wrap:hover .ticker-move {{ animation-play-state: paused; }} 
    @keyframes ticker-scroll {{ 0% {{ transform: translate3d(0, 0, 0); }} 100% {{ transform: translate3d(-50%, 0, 0); }} }} 
    .ticker-item {{ display: inline-block; padding: 0 2rem; border-right: 1px solid {C_CARD_BORDER}; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }} 
    .tape-lbl {{ font-weight: 700; color: {C_TEXT_SEC}; margin-right: 6px; }} 
    .tape-val {{ font-weight: 800; color: {C_NAVY}; font-size: 15px; }} 
    .t-pos, .t-neg {{ font-size: 13px !important; font-weight: 700; padding: 2px 8px; border-radius: 6px; margin-left: 10px; }} 
    .t-pos {{ color: #059669; background-color: #ECFDF5; }} .t-neg {{ color: #DC2626; background-color: #FEF2F2; }} 
    </style> 
    """ 
    st.markdown(f'{css_animation}<div class="ticker-wrap"><div class="ticker-move">{full_content}</div></div>', unsafe_allow_html=True) 
 
def kpi_card_dual(title, value, suffix="", label_text="Neutro", label_color="blue", right_content_html=""): 
    html = f"<div class='kpi-card'><div style='display:flex; justify-content:space-between; align-items:center;'><div><div class='kpi-title'>{title}</div><div class='kpi-val'>{value}<span style='font-size:1.2rem; color:{C_TEXT_SEC}; margin-left:2px;'>{suffix}</span></div><div style='margin-top:6px;'><span class='badge badge-{label_color}'>{label_text}</span></div></div>{right_content_html}</div></div>" 
    st.markdown(html, unsafe_allow_html=True) 
 
def kpi_card_simple(title, value, suffix="", label_text="Neutro", label_color="blue"): 
    html = f"<div class='kpi-card'><div class='kpi-top'><div class='kpi-title'>{title}</div><div class='kpi-val'>{value}<span style='font-size:0.6em; color:{C_TEXT_SEC}; margin-left:2px;'>{suffix}</span></div></div><div class='kpi-bottom' style='margin-top:6px;'><span class='badge badge-{label_color}'>{label_text}</span></div></div>" 
    st.markdown(html, unsafe_allow_html=True) 
 
def make_yoy_html(delta): 
    if delta is None: return "" 
    d_col, d_sym = (C_GREEN_TEXT, "▲") if delta >= 0 else (C_RED_TEXT, "▼") 
    return f"<div style='text-align:right;'><div style='font-size:1.2rem; font-weight:700; color:{d_col};'>{d_sym} {abs(delta):.1f}%</div><div style='font-size:0.8rem; color:{C_TEXT_SEC}; font-weight:500;'>vs Año Ant.</div></div>" 
 
def make_simple_right_html(val_text, label_text, color=C_TEXT): 
    return f"<div style='text-align:right;'><div style='font-size:1.2rem; font-weight:700; color:{color};'>{val_text}</div><div style='font-size:0.8rem; color:{C_TEXT_SEC}; font-weight:500;'>{label_text}</div></div>" 
 
def interpolate(val, x0, y0, x1, y1): 
    return y0 + (val - x0) * (y1 - y0) / (x1 - x0) 
 
def calculate_needle_angle(score): 
    safe_score = max(1.0, min(5.0, float(score))) 
    if safe_score <= 2.0: return interpolate(safe_score, 1.0, 200, 2.0, 130) 
    elif safe_score <= 3.0: return interpolate(safe_score, 2.0, 135, 3.0, 90) 
    elif safe_score <= 4.0: return interpolate(safe_score, 3.0, 90, 4.0, 50) 
    else: return interpolate(safe_score, 4.0, 45, 5.0, -20) 
 
# ========================================== 
# 4. LÓGICA PRINCIPAL 
# ========================================== 
if df_kpi.empty: 
    st.warning("⚠️ Esperando datos...") 
    st.stop() 
 
all_tickers = df_kpi['Ticker'].unique().tolist() 
qp = st.query_params 

if 'wl_backup' in qp: 
    st.session_state['watchlist'] = [x for x in qp['wl_backup'].split(',') if x] 
elif 'watchlist' not in st.session_state: 
    st.session_state['watchlist'] = ['AAPL', 'NVDA','BCI.SN','MSFT'] 
 
if 'selected_ticker' not in st.session_state: 
    st.session_state.selected_ticker = all_tickers[0] if all_tickers else 'AAPL' 
 
try: 
    should_clear = False
    if 'sel_ticker' in qp: 
        if qp['sel_ticker'] in all_tickers: 
            st.session_state.selected_ticker = qp['sel_ticker'] 
            st.session_state.search_box = qp['sel_ticker'] 
        should_clear = True
    if 'del_ticker' in qp: 
        if qp['del_ticker'] in st.session_state['watchlist']: 
            st.session_state['watchlist'].remove(qp['del_ticker']) 
        should_clear = True
    
    if should_clear:
        st.query_params.clear()
except: pass 
 
current_ticker = st.session_state.selected_ticker 
current_kpi_row = df_kpi[df_kpi['Ticker'] == current_ticker].iloc[0] 
is_index = (
    current_ticker.startswith('^') or 
    'BTC' in current_ticker or 
    'ETH' in current_ticker or 
    'IPSA' in current_ticker or 
    'S&P' in current_ticker or 
    'Nasdaq' in current_ticker or 
    'Dow' in current_ticker
)
 
hist_subset = df_hist[df_hist['Ticker'] == current_ticker].sort_values('Date') 
hist_price = hist_subset[hist_subset['Type'] == 'Price'] 
hist_fin = hist_subset[hist_subset['Type'] == 'Financials'] 
 
# ========================================== 
# 5. SIDEBAR 
# ========================================== 
with st.sidebar: 
    # TOGGLE
    col_t1, col_t2 = st.columns([0.7, 0.3], vertical_alignment="center")
    with col_t1: st.markdown("### 🏛️ PORTAL INDICADORES")
    with col_t2:
        is_dark = st.session_state['theme'] == 'Dark'
        if st.toggle("Dark", value=is_dark, label_visibility="collapsed"):
            if not is_dark:
                st.session_state['theme'] = 'Dark'
                st.rerun()
        else:
            if is_dark:
                st.session_state['theme'] = 'Light'
                st.rerun()

    col_search, col_add = st.columns([0.815, 0.185], gap="small", vertical_alignment="bottom") 
    with col_search: 
        target = st.session_state.get('selected_ticker', all_tickers[0])
        if target in all_tickers: st.session_state.search_box = target
        def upd(): st.session_state.selected_ticker = st.session_state.search_box 
        st.selectbox("BUSCAR ACTIVO", all_tickers, key='search_box', on_change=upd, label_visibility="collapsed") 
    with col_add: 
        def add(): 
            if st.session_state.search_box not in st.session_state['watchlist']: 
                st.session_state['watchlist'].append(st.session_state.search_box) 
        st.button("+", on_click=add, use_container_width=True) 
 
    st.markdown("<div class='sidebar-sep'></div>", unsafe_allow_html=True) 
    if st.session_state['watchlist']: 
        df_wl = get_watchlist_data(st.session_state['watchlist'], df_hist) 
        bk = ",".join(st.session_state['watchlist']) 
        ct = st.session_state['theme']
        html_rows = "" 
        for _, row in df_wl.iterrows(): 
            s, l, c = row['Symbol'], row['Last'], row['Chg%'] 
            price_str = f"{l:,.0f}" if l >= 10000 else f"{l:,.2f}" 
            c_cls, c_sym = ("wl-up", "+") if c >= 0 else ("wl-down", "") 
            # Fix: Pass current ticker (sel_ticker) in delete link to avoid reset
            row_html = f"""<a href="?sel_ticker={s}&wl_backup={bk}&theme={ct}" target="_self" class="wl-row"><div class="wl-sym">{s}</div><div class="wl-price">{price_str}</div><div class="wl-col-end"><span class="sidebar-percent {c_cls}">{c_sym}{c:.2f}%</span><object><a href="?del_ticker={s}&wl_backup={bk}&theme={ct}&sel_ticker={current_ticker}" target="_self" class="wl-del">✕</a></object></div></a>""" 
            html_rows += row_html 
        st.markdown(f"""<div class="wl-container"><div class="sidebar-header wl-title">LISTA DE SEGUIMIENTO</div><div class="wl-header"><span style="text-align:left">ACTIVO</span><span style="text-align:right">PRECIO</span><span style="text-align:center">VAR %</span></div>{html_rows}</div>""", unsafe_allow_html=True) 
    else: st.info("Lista vacía.") 
 
    st.markdown("<div class='sidebar-sep'></div>", unsafe_allow_html=True) 
    avail = sorted(df_hist[df_hist['Type'].isin(['Price', 'Benchmark'])]['Ticker'].unique().tolist()) 
    if current_ticker in avail: avail.remove(current_ticker) 
    st.markdown("<div class='sidebar-header'>COMPARAR RENDIMIENTO</div>", unsafe_allow_html=True)
    compare_market = st.selectbox("COMPARAR RENDIMIENTO", ["Sin Comparación"] + avail, label_visibility="collapsed") 
    st.markdown("<div class='sidebar-sep'></div>", unsafe_allow_html=True) 
 
    # FUNDAMENTALS 
    h52, l52 = float(current_kpi_row.get('52W_High', 0)), float(current_kpi_row.get('52W_Low', 0)) 
    perf_ytd, perf_1y = float(current_kpi_row.get('Perf_YTD', 0)), float(current_kpi_row.get('Perf_1Y', 0)) 
    curr_price_kpi = float(current_kpi_row.get('Precio', 0)) 
    if not is_index: 
        try: 
            mkt_cap = format_market_cap(float(current_kpi_row.get('Utilidad_Neta', 0)) * float(current_kpi_row.get('PE_Ratio', 0))) 
            dy = float(current_kpi_row.get('Dividend_Yield', 0)) * 100 
            dy_str, dy_col = f"{dy:.2f}%", "#10B981" if dy > 0.1 else "#94A3B8" 
        except: mkt_cap, dy_str, dy_col = "N/A", "--", "#94A3B8" 
    else: mkt_cap, dy_str, dy_col = "N/A", "--", "#94A3B8" 
 
    with st.expander("**📊 DATOS FUNDAMENTALES**", expanded=False): 
        st.markdown(f"<div class='sidebar-stat'><div class='sidebar-label'>Sector</div><div class='sidebar-val' style='font-size:0.8rem; color:{C_TEXT_SEC} !important'>{current_kpi_row.get('Sector', 'N/A')}</div></div><div class='sidebar-stat'><div class='sidebar-label'>Market Cap (Est)</div><div class='sidebar-val'>{mkt_cap}</div></div><div class='sidebar-stat' style='border:none'><div class='sidebar-label'>Dividend Yield</div><div class='sidebar-val' style='color:{dy_col} !important'>{dy_str}</div></div>", unsafe_allow_html=True) 
        sym_ytd, c_ytd = ("▲", "#16a34a") if perf_ytd >= 0 else ("▼", "#dc2626") 
        sym_1y, c_1y = ("▲", "#16a34a") if perf_1y >= 0 else ("▼", "#dc2626") 
        st.markdown(f"<div class='sidebar-stat'><div class='sidebar-label'>YTD Return</div><div style='font-family:Segoe UI; font-size:0.9rem; font-weight:800; color:{c_ytd}; text-align:left;'>{sym_ytd} {perf_ytd:+.2f}%</div></div><div class='sidebar-stat'><div class='sidebar-label'>1 Year Return</div><div style='font-family:Segoe UI; font-size:0.9rem; font-weight:800; color:{c_1y}; text-align:left;'>{sym_1y} {perf_1y:+.2f}%</div></div>", unsafe_allow_html=True) 
 
    # 52 WEEK (SOLO MODIFICACIÓN DE ALINEACIÓN - FLEXBOX)
    st.markdown("<div class='sidebar-header' style='margin-top:15px;'>RANGO 52 SEMANAS</div>", unsafe_allow_html=True) 
    d_high = ((curr_price_kpi - h52) / h52) * 100 if h52 > 0 else 0 
    d_low = ((curr_price_kpi - l52) / l52) * 100 if l52 > 0 else 0 
    st.markdown(f""" 
    <div style='margin-bottom:10px; border-bottom:1px solid {C_CARD_BORDER}; padding-bottom:10px;'> 
        <div class='sidebar-label' style='margin-bottom:4px;'>52W HIGH</div> 
        <div style='display:flex; justify-content:space-between; align-items:center;'> 
            <div class='sidebar-val'>${h52:,.2f}</div> 
            <span class='sidebar-percent' style='color:#dc2626; background-color: #FEF2F2;'>▼ {d_high:.1f}%</span> 
        </div> 
    </div> 
    <div style='margin-bottom:0px;'> 
        <div class='sidebar-label' style='margin-bottom:4px;'>52W LOW</div> 
        <div style='display:flex; justify-content:space-between; align-items:center;'> 
            <div class='sidebar-val'>${l52:,.2f}</div> 
            <span class='sidebar-percent' style='color:#16a34a; background-color: #ECFDF5;'>▲ +{d_low:.1f}%</span> 
        </div> 
    </div> 
    """, unsafe_allow_html=True) 
 
    st.markdown("<div class='sidebar-sep'></div>", unsafe_allow_html=True) 
    tooltip_text = ( 
        "¿QUÉ ES EL SENTIMIENTO DE MERCADO?&#10;" 
        "Mide si las acciones tienen un precio justo. La teoría es que el miedo hunde los precios y la codicia los infla.&#10;&#10;" 
        "¿CÓMO SE CALCULA?&#10;" 
        "Combina 7 indicadores: Momentum, Fortaleza, Amplitud, Opciones Put/Call, Bonos Basura, Volatilidad y Demanda de Refugio. (0=Miedo Extremo, 100=Codicia Extrema).&#10;&#10;" 
        "¿PARA QUÉ SIRVE?&#10;" 
        "Para medir el ánimo emocional del mercado. Ayuda a los inversores a identificar sus propios sesgos emocionales y evitar sobrerreacciones." 
    ) 

    # Lógica de colores (igual que antes)
    try: 
        fgi = df_macro[df_macro['Indicador'] == 'F&G Index'] 
        fgi_v = float(fgi['Valor'].iloc[0]) if not fgi.empty else 50.0 
    except: fgi_v = 50.0 
    
    if fgi_v < 25: fgi_c, fgi_t = G_RED, "EXTREME FEAR" 
    elif fgi_v < 45: fgi_c, fgi_t = G_ORANGE, "FEAR" 
    elif fgi_v < 55: fgi_c, fgi_t = G_YELLOW, "NEUTRAL" 
    elif fgi_v < 75: fgi_c, fgi_t = G_LGREEN, "GREED" 
    else: fgi_c, fgi_t = G_DGREEN, "EXTREME GREED" 
 
    # CABECERA CON EL ICONO DE INFORMACIÓN (i) RESTAURADO
    st.markdown(f""" 
    <div style='margin-top:20px; display:flex; align-items:center; gap:6px; margin-bottom:5px;'> 
        <div class='sidebar-header' style='margin-bottom:0px; line-height:1;'>SENTIMIENTO DE MERCADO (CNN)</div> 
        <div title="{tooltip_text}" style='cursor:help; font-size:0.9rem; color:#F59E0B; opacity:0.6; display:flex; align-items:center;transform: translateY(-5px);font-weight: 900;'> 
            &#9432;  
        </div>  
    </div> 
    """, unsafe_allow_html=True) 
    fig_fgi = go.Figure(go.Indicator(
        mode = "gauge+number", 
        value = fgi_v, 
        number = {'font': {'size': 32, 'color': fgi_c, 'weight': 'bold'}}, 
        gauge = {
            'axis': {'range': [0, 100], 'tickwidth': 2, 'tickcolor': C_CHART_TEXT, 'tickfont': {'size': 12, 'weight': 'bold', 'color': C_CHART_TEXT}}, 
            'bar': {'color': fgi_c}, 'bgcolor': "white", 'borderwidth': 0, 
            'steps': [{'range': [0, 25], 'color': "#fee2e2"}, {'range': [25, 45], 'color': "#ffedd5"}, {'range': [45, 55], 'color': "#fef9c3"}, {'range': [55, 75], 'color': "#dcfce7"}, {'range': [75, 100], 'color': "#d1fae5"}]
        }
    )) 
    fig_fgi.update_layout(height=140, margin=dict(t=30, b=10, l=15, r=30), paper_bgcolor='rgba(0,0,0,0)', font={'family': "Segoe UI"}) 
    st.plotly_chart(fig_fgi, use_container_width=True, config={'displayModeBar': False}) 
    st.markdown(f"<div style='text-align:center; font-weight:800; font-size:1.2rem; color:{fgi_c}; margin-top:-40px;margin-left:-7px;'>{fgi_t}</div>", unsafe_allow_html=True) 
 
# ========================================== 
# 6. MAIN CONTENT 
# ========================================== 
display_ticker_tape(df_macro) 
c_head_1, c_head_2 = st.columns([2, 1]) 
with c_head_1: 
    st.markdown(f"<div style='display:flex; align-items:baseline; gap:8px;'><span class='header-ticker'>{current_ticker}</span><span style='color:{C_TEXT_SEC}; font-size:1.5rem; font-weight:400;'>{'Index/ETF' if is_index else 'Equity'}</span></div>", unsafe_allow_html=True) 
with c_head_2: 
    if not hist_price.empty: 
        curr = float(hist_price.iloc[-1]['Close']) 
        prev = float(hist_price.iloc[-2]['Close']) if len(hist_price) > 1 else curr 
        delta = ((curr - prev)/prev)*100 if prev != 0 else 0 
        c_h = C_GREEN_TEXT if delta >= 0 else C_RED_TEXT 
        c_b = "#ECFDF5" if delta >= 0 else "#FEF2F2"
        sym = "+" if delta >= 0 else "" 
        st.markdown(f"<div style='display:flex; flex-direction:column; align-items:flex-end;'><div style='display:flex; align-items:baseline; gap:10px;'><span style='font-size:2.5rem; font-weight:700; color:{C_NAVY};'>${curr:,.2f}</span><span style='color:{c_h}; font-weight:700; font-size:1.5rem; background-color:{c_b}; padding:0px 6px; border-radius:4px;transform: translateY(-4px);'>{sym}{delta:.2f}%</span></div></div>", unsafe_allow_html=True) 
 
delta_rev, delta_net = 0.0, 0.0 
if not is_index and len(hist_fin) >= 2: 
    try: 
        lr, pr = float(hist_fin.iloc[-1]['Metric_Revenue']), float(hist_fin.iloc[-2]['Metric_Revenue']) 
        ln, pn = float(hist_fin.iloc[-1]['Metric_NetIncome']), float(hist_fin.iloc[-2]['Metric_NetIncome']) 
        if pr != 0: delta_rev = ((lr - pr) / pr) * 100 
        if pn != 0: delta_net = ((ln - pn) / abs(pn)) * 100 
    except: pass 
 
k1, k2, k3, k4, k5, k6 = st.columns(6) 
# --- LÓGICA DE ETIQUETAS MEJORADA (HUMAN READABLE) ---
if not is_index: 
    # Extraer valores
    roe = float(current_kpi_row.get('ROE', 0)) 
    ebitda = float(current_kpi_row.get('EBITDA', 0)) 
    pe = float(current_kpi_row.get('PE_Ratio', 0)) 
    peg = float(current_kpi_row.get('PEG_Ratio', 0)) 
    beta = float(current_kpi_row.get('Beta', 1.0)) 
    act = float(current_kpi_row.get('Activo_Cte', 0))
    pas = float(current_kpi_row.get('Pasivo_Cte', 1))
    cr = act / pas if pas > 0 else 0
    qr = float(current_kpi_row.get('Quick_Ratio', 0)) 

    # 1. Lógica ROE (Rentabilidad)
    if roe > 20: lbl_roe, col_roe = "Alta Rentabilidad", "green"
    elif roe > 10: lbl_roe, col_roe = "Estable", "blue"
    else: lbl_roe, col_roe = "Baja Rentabilidad", "red"

    # 2. Lógica EBITDA
    lbl_ebitda, col_ebitda = ("Operativa", "green") if ebitda > 0 else ("Pérdidas Op.", "red")

    # 3. Lógica P/E (AVANZADA: CRITERIO GROWTH vs VALUE)
    # Detectamos si el precio alto está justificado por crecimiento
    if pe <= 0: 
        lbl_pe, col_pe = "Sin Utilidades", "red"
    elif 0 < pe < 20: 
        lbl_pe, col_pe = "Value / Barata", "green"
    elif 20 <= pe <= 35: 
        lbl_pe, col_pe = "Razonable", "blue"
    else: 
        # PE > 35 (Aquí es donde discriminamos Growth vs Burbuja)
        # Si el PEG es bajo (< 2.0), significa que las ganancias crecen rápido
        if 0 < peg < 2.0:
            lbl_pe, col_pe = "High Growth", "green"  # <--- NVDA, SOFI entran aquí
        else:
            lbl_pe, col_pe = "Sobrevalorada", "yellow"    # <--- Precio alto sin crecimiento que lo respalde

    # 4. Lógica PEG (Ajuste fino)
    if peg <= 0: lbl_peg, col_peg = "N/A", "grey"
    elif peg < 1.0: lbl_peg, col_peg = "Subvaluada", "green" # Growth at reasonable price
    elif peg < 2.0: lbl_peg, col_peg = "Precio Justo", "blue" # Normal para Tech
    else: lbl_peg, col_peg = "Sobrevalorada", "red"

    # 5. Lógica BETA (Volatilidad)
    if beta < 0.8: lbl_beta, col_beta = "Defensiva", "blue" # Se mueve poco
    elif beta > 1.3: lbl_beta, col_beta = "Volátil", "yellow" # Se mueve mucho
    else: lbl_beta, col_beta = "Mercado", "blue" # Se mueve igual al S&P

    # 6. Lógica LIQUIDEZ
    lbl_liq, col_liq = ("Solvente", "green") if cr > 1.2 else ("Ajustada", "red")
    col_qr = C_GREEN_TEXT if qr > 1 else C_RED_TEXT

    # --- RENDERIZADO DE TARJETAS ---
    with k1: kpi_card_dual("RENTABILIDAD (ROE)", f"{roe:.1f}", "%", lbl_roe, col_roe, make_yoy_html(delta_net)) 
    with k2: kpi_card_dual("EBITDA (TTM)", f"${ebitda/1e9:.1f}", "B", lbl_ebitda, col_ebitda, make_yoy_html(delta_rev)) 
    with k3: kpi_card_dual("VALORACIÓN (P/E)", f"{pe:.1f}", "x", lbl_pe, col_pe, make_yoy_html(perf_1y)) 
    with k4: kpi_card_dual("PEG RATIO", f"{peg:.2f}", "x", lbl_peg, col_peg, make_simple_right_html(f"{(pe/peg if peg!=0 else 0):.1f}%", "Growth Est.", C_SIDEBAR)) 
    with k5: kpi_card_dual("RIESGO (BETA)", f"{beta:.2f}", "", lbl_beta, col_beta, make_simple_right_html("1.00", "Ref. S&P", "#94A3B8")) 
    with k6: kpi_card_dual("LIQUIDEZ", f"{cr:.2f}", "x", lbl_liq, col_liq, make_simple_right_html(f"{qr:.2f}x", "Quick Ratio", col_qr)) 

else: 
    # MODO ÍNDICES (SIN CAMBIOS)
    with k1: kpi_card_simple("RETORNO YTD", f"{perf_ytd:+.2f}", "%", "Positivo" if perf_ytd>0 else "Negativo", "green" if perf_ytd>0 else "red") 
    with k2: kpi_card_simple("RETORNO 1 AÑO", f"{perf_1y:+.2f}", "%", "Anual", "green" if perf_1y>0 else "red") 
    with k3: kpi_card_simple("DRAWDOWN (52W)", f"{((curr_price_kpi-h52)/h52)*100:.1f}", "%", f"High: {h52:,.0f}", "blue") 
    with k4: kpi_card_simple("VOLATILIDAD", "N/A", "", "Standard", "blue")
 
st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True) 
with st.container(border=True, height=350): 
    hist_price['Date'] = pd.to_datetime(hist_price['Date']) 
    fig = go.Figure() 
    y_tit, tick_suf, r_min, r_max = "Precio ($)", "", 0, 100 
    if compare_market != "Sin Comparación": 
        hb = df_hist[df_hist['Ticker'] == compare_market].sort_values('Date').drop_duplicates(subset=['Date'], keep='last') 
        if not hb.empty: 
            hb['Date'] = pd.to_datetime(hb['Date']).dt.tz_localize(None) 
            hist_price['Date'] = hist_price['Date'].dt.tz_localize(None) 
            mg = pd.merge(hist_price, hb, on='Date', suffixes=('', '_b'), how='inner') 
            if not mg.empty: 
                b1, b2 = mg['Close'].iloc[0], mg['Close_b'].iloc[0] 
                mg['p1'] = ((mg['Close'] - b1)/b1)*100 
                mg['p2'] = ((mg['Close_b'] - b2)/b2)*100 
                fig.add_trace(go.Scatter(x=mg['Date'], y=mg['p1'], mode='lines', name=current_ticker, line=dict(color=C_BLUE_REV, width=3), fill='tozeroy', fillcolor=C_BLUE_SOFT)) 
                fig.add_trace(go.Scatter(x=mg['Date'], y=mg['p2'], mode='lines', name=compare_market, line=dict(color=C_COMPARE, width=1.5, dash='solid'))) 
                y_tit, tick_suf = "Rendimiento (%)", "%" 
            else: fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['Close'], mode='lines', name=current_ticker, line=dict(color=C_BLUE_REV, width=3), fill='tozeroy', fillcolor=C_BLUE_SOFT)) 
        else: fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['Close'], mode='lines', name=current_ticker, line=dict(color=C_BLUE_REV, width=3), fill='tozeroy', fillcolor=C_BLUE_SOFT)) 
    else: 
        fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['Close'], mode='lines', name=current_ticker, line=dict(color=C_BLUE_REV, width=3), fill='tozeroy', fillcolor=C_BLUE_SOFT)) 
        if 'SMA_20' in hist_price: fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['SMA_20'], mode='lines', name='SMA 20', line=dict(color=C_SMA_20, width=1.5))) 
        if 'SMA_50' in hist_price: fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['SMA_50'], mode='lines', name='SMA 50', line=dict(color=C_SMA_50, width=1.5))) 
        if 'SMA_200' in hist_price: fig.add_trace(go.Scatter(x=hist_price['Date'], y=hist_price['SMA_200'], mode='lines', name='SMA 200', line=dict(color=C_SMA_200, width=1.5), visible='legendonly')) 
     
    if fig.data: 
        yv = [] 
        for t in fig.data: 
            if t.y is not None: yv.extend(t.y) 
        if yv: 
            mn, mx = min(yv), max(yv) 
            mg = (mx - mn)*0.1 
            r_min, r_max = mn - mg, mx + mg 
 
    fig.add_vline(x="2025-04-04", line_width=1.5, line_dash="dash", line_color="#EF4444") 
    fig.add_annotation(x="2025-04-04", y=1, yref="paper", text="Liberation Day", showarrow=False, 
                           font=dict(color="#EF4444", size=12, weight="bold"), xanchor="left", xshift=5, yshift=-5)
    fig.update_layout(
        title=dict(text=f"<b>EVOLUCIÓN ({'COMPARATIVA' if 'Sin' not in compare_market and len(fig.data)>1 else 'PRECIO'})</b>", font=dict(size=14, color=C_SIDEBAR_TITLE, family="Segoe UI"), x=0.01, y=0.98), 
        paper_bgcolor=C_CARD_BG, plot_bgcolor=C_CARD_BG, height=320, 
        margin=dict(t=30, b=0, l=0, r=0), 
        xaxis=dict(showgrid=False, linecolor=C_CARD_BORDER, tickfont=dict(color=C_CHART_TEXT, size=11, weight='bold'), tickformat="%b %Y", hoverformat="%d %b %Y"), 
        yaxis=dict(range=[r_min, r_max], showgrid=True, gridcolor=C_CHART_GRID, gridwidth=1, tickfont=dict(color=C_CHART_TEXT, size=11, weight='bold'), title=None, ticksuffix=tick_suf), 
        hovermode="x unified", showlegend=True, 
        legend=dict(orientation="h", yanchor="top", y=1.12, xanchor="right", x=0.31, font=dict(size=12, color=C_CHART_TEXT), itemclick="toggle", itemdoubleclick="toggleothers")
    ) 
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False}) 
 
if not is_index: 
    c1, c2 = st.columns([1, 2]) 
    with c1: 
       with st.container(border=True, height=330): 
        st.markdown(f"<div style='font-size:1.3rem; font-weight:800; color:{C_NAVY}; margin-bottom:15px;'>CONSENSO DE ANALISTAS</div>", unsafe_allow_html=True)   
        cur, tar = float(current_kpi_row.get('Precio', 0)), float(current_kpi_row.get('Target_Price', 0)) 
        num, sco = int(current_kpi_row.get('Num_Analysts', 0)), float(current_kpi_row.get('Consensus_Score', 3.0)) 
        ang = calculate_needle_angle(sco) 
        cx, cy, rad = 0.5, 0.2, 0.35 
        rd = math.radians(ang) 
        xh, yh = cx + rad * math.cos(rd), cy + rad * math.sin(rd) 
        path = f"M {xh} {yh} L {cx+0.04*math.cos(rd+math.pi/2)} {cy+0.04*math.sin(rd+math.pi/2)} L {cx+0.04*math.cos(rd-math.pi/2)} {cy+0.04*math.sin(rd-math.pi/2)} Z" 
        if sco >= 4.2: rt, rc = "COMPRA FUERTE", G_DGREEN   
        elif sco >= 3.4: rt, rc = "COMPRAR", G_LGREEN   
        elif sco >= 2.6: rt, rc = "MANTENER", C_AMBER_TEXT  
        elif sco >= 1.8: rt, rc = "VENDER", G_ORANGE   
        else: rt, rc = "VENTA FUERTE", G_RED   
        up = ((tar - cur) / cur) * 100 if cur > 0 else 0 
        us, uc = ("▲", C_GREEN_TEXT) if up > 0 else ("▼", C_RED_TEXT) 
        cd, cv = st.columns([9.5, 20])   
        with cd: st.markdown(f"<div style='height: 10px;'></div></br><div style='line-height: 1.1;'><div style='font-size: 0.9rem; color: {C_TEXT_SEC}; font-weight: 600; margin-bottom:4px;'>OBJETIVO (12M)</div><div style='font-size: 2.0rem; font-weight: 800; color: {C_NAVY}; margin-bottom: 5px;'>${tar:.2f}</div><span style='background-color: {uc}15; color: {uc}; padding: 3px 8px; border-radius: 6px;margin-top: 10px; font-weight: 700; font-size: 1.1rem;'>{us} {up:.1f}% Potencial</span><div style='color: {C_TEXT_SEC}; font-size: 1rem; margin-top: 15px;'>Actual: ${cur:.2f}</div><div style='color: {C_TEXT_SEC}; font-size: 1rem; margin-top: 2px;'>Base: {num} Analistas.</div></div>", unsafe_allow_html=True)   
        with cv:  
            fg = go.Figure(go.Indicator(
                mode = "gauge", 
                value = sco, 
                gauge = {
                    'axis': {'range': [1, 5], 'tickvals': [1, 2, 3, 4, 5], 'ticktext': ['<b>1</b>', '<b>2</b>', '<b>3</b>', '<b>4</b>', '<b>5</b>'], 'tickwidth':2, 'tickcolor': C_CHART_TEXT, 'tickfont': {'size': 20, 'color': C_CHART_TEXT}}, 
                    'bar': {'color': "rgba(0,0,0,0)"}, 'bgcolor': "white", 'borderwidth': 0, 
                    'steps': [{'range': [1, 1.8], 'color': G_RED}, {'range': [1.8, 2.6], 'color': G_ORANGE}, {'range': [2.6, 3.4], 'color': G_YELLOW}, {'range': [3.4, 4.2], 'color': G_LGREEN}, {'range': [4.2, 5], 'color': G_DGREEN}]
                }
            ))  
            fg.add_shape(type="path", path=path, fillcolor=C_NAVY, line_color=C_NAVY)  
            fg.add_shape(type="circle", x0=cx-0.03, y0=cy-0.03, x1=cx+0.03, y1=cy+0.03, fillcolor=C_NAVY, line_color=C_NAVY)  
            fg.add_annotation(x=0.5, y=-0.05, text=f"<b>{rt}</b>", showarrow=False, font=dict(size=25, color=rc)) 
            fg.update_layout(height=190, margin=dict(t=20, b=5, l=20, r=20), paper_bgcolor='rgba(0,0,0,0)', font={'family': "Segoe UI"})  
            st.plotly_chart(fg, use_container_width=True, config={'displayModeBar': False}) 
            st.markdown(f"<div style='text-align: center; margin-top: -5px;'><div style='font-size:1.5rem; font-weight:bold; color:{C_TEXT_SEC};'>Calificación: <span style='color:{C_SIDEBAR}'>{sco:.1f}</span></div></div>", unsafe_allow_html=True) 
     
    with c2: 
        with st.container(border=True, height=330): 
            st.markdown(f"<div style='font-size:1.3rem; font-weight:800; color:{C_NAVY}; margin-bottom:0px;'>ESTRUCTURA DE RESULTADOS (BRIDGE)</div>", unsafe_allow_html=True) 
            
            # 1. CÁLCULOS
            rv = float(current_kpi_row.get('Ventas', 0)) 
            rc = float(current_kpi_row.get('Costo_Ventas', 0)) 
            cg = -rc if rc > 0 else (-(rv * 0.4) if rv > 0 else rc) 
            gr = rv + cg 
            nt = float(current_kpi_row.get('Utilidad_Neta', 0)) 
            gp = nt - gr 
            gl, gn = (f"-${abs(gp)/1e9:.1f}B", "Gastos/Imp.") if gp < 0 else (f"+${abs(gp)/1e9:.1f}B", "Otros") 
            mb = (gr / rv * 100) if rv else 0 
            mn = (nt / rv * 100) if rv else 0 
            def fb(v): return f"${abs(v)/1e9:.1f}B" 
            ls = f"color:{C_NAVY}; font-weight:900; font-size:13px;" 
            
            # Definición de etiquetas
            tx = [
                f"<span style='{ls}'>{fb(rv)}</span>", 
                f"<span style='{ls}'>-{fb(cg)}</span>", 
                f"<span style='{ls}'>{fb(gr)}</span><br><span style='color:{C_TEXT_SEC}; font-size:11px'>({mb:.1f}%)</span>", 
                f"<span style='{ls}'>{gl}</span>", 
                f"<span style='{ls}'>{fb(nt)}</span><br><span style='color:{C_TEXT_SEC}; font-size:11px'>({mn:.1f}%)</span>"
            ] 
            
            mv = max(rv, gr, nt) 

            # --- LÓGICA PARA FORZAR ETIQUETA ARRIBA ---
            # Si gp es negativo, guardamos el texto y lo borramos de la lista 'tx' 
            # para dibujarlo manualmente con una anotación.
            gap_label_manual = None
            if gp < 0:
                gap_label_manual = tx[3]
                tx[3] = "" 

            # 2. CREACIÓN DEL GRÁFICO
            fw = go.Figure(go.Waterfall(
                name = "P&L", orientation = "v", 
                measure = ["relative", "relative", "total", "relative", "total"], 
                x = ["Ingresos", "Costo Ventas", "Utilidad Bruta", gn, "Utilidad Neta"], 
                y = [rv, cg, 0, gp, 0], 
                text = tx, 
                textposition = "outside", # Ahora usamos 'outside' general porque ocultamos el problemático
                textfont=dict(color=C_CHART_TEXT), 
                cliponaxis = False, 
                connector = {"line":{"color":"#8E96A1", "width":2, "dash":"dot"}}, 
                totals = {"marker":{"color": C_FOREST}}, 
                decreasing = {"marker":{"color": C_RED_EXP}}, 
                increasing = {"marker":{"color": C_BLUE_REV}}
            )) 

            if gp < 0 and gap_label_manual:
                fw.add_annotation(
                    x=gn, # Columna "Gastos/Imp."
                    y=gr, # Altura donde empieza a caer (Utilidad Bruta)
                    text=gap_label_manual,
                    showarrow=False,
                    yshift=25 # Empujamos 25px hacia arriba para que quede flotando bien
                )
            
            # --- LAYOUT FINAL ---
            fw.update_layout(
                height=290, 
                margin=dict(t=14, b=30, l=0, r=0), 
                paper_bgcolor='rgba(0,0,0,0)', 
                plot_bgcolor=C_CARD_BG, 
                font=dict(family="Segoe UI"), 
                yaxis=dict(showgrid=True, gridcolor=C_CHART_GRID, gridwidth=1, showline=False, showticklabels=False, range=[0, mv * 1.35 if mv > 0 else 100]), # Aumenté rango a 1.35 para dar espacio extra arriba
                xaxis=dict(showline=True, linecolor=C_CARD_BORDER, tickfont=dict(size=14, color=C_TEXT_SEC, weight="bold"))
            ) 
            st.plotly_chart(fw, use_container_width=True, config={'displayModeBar': False})
 
st.markdown(f"<br><div style='text-align: center; color: {C_TEXT_SEC}; font-size: 10px;'>Terminal Inversionista v2.0 (Theme System) | Datos Diferidos</div>", unsafe_allow_html=True)