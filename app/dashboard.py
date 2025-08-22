# /app/dashboard.py

# --- INÍCIO DO BLOCO DE AJUSTE DE PATH --- #
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM DO BLOCO DE AJUSTE DE PATH --- #

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import joblib
from datetime import datetime, timedelta

from scripts.database import get_data_from_db

# Configuração da página
st.set_page_config(page_title="Dados Meteorológicos", layout="wide")

# Título do Dashboard
st.title("🌦️ Dashboard Meteorológico - São Luiz do Paraitinga")
st.markdown("Dados da estação INMET A740 atualizados automaticamente")

# Filtros na barra lateral
st.sidebar.header("Filtros")
default_start = datetime.now() - timedelta(days=30)
date_range = st.sidebar.date_input("Período", [default_start.date(), datetime.now().date()])
show_raw = st.sidebar.checkbox("Mostrar dados brutos")
show_predictions = st.sidebar.checkbox("Mostrar previsões")
show_model_info = st.sidebar.checkbox("Mostrar informações do modelo")

# Função para carregar os dados do banco de dados
@st.cache_data(ttl=3600, show_spinner="Carregando dados...")
def load_data():
    """Busca os dados do banco de dados, solicitando apenas as colunas existentes."""
    query = """
        SELECT 
            data, hora, precipitacao_total, pressao_atm_estacao,
            temperatura_ar, umidade_relativa, vento_velocidade,
            vento_direcao, radiacao_global, temperatura_max, temperatura_min
        FROM meteo_data
        ORDER BY data DESC, hora DESC
    """
    return get_data_from_db(query)

# Carrega os dados
df = load_data()

# Validação para interromper a execução se os dados não forem carregados
if df.empty:
    st.error("Não foi possível carregar os dados do banco de dados. Verifique as configurações de conexão e se a tabela contém dados.")
    st.stop()

# Processar filtros de data
if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    df['data'] = pd.to_datetime(df['data'])
    df = df[(df["data"].dt.date >= start_date.date()) & (df["data"].dt.date <= end_date.date())]

# CORREÇÃO: Mover a criação de colunas para fora do bloco 'if'
# e garantir que as colunas de origem existam.
if 'data' in df.columns and 'hora' in df.columns:
    df["datetime"] = pd.to_datetime(df["data"].astype(str) + " " + df["hora"].astype(str))
    df["hour"] = df["datetime"].dt.hour
    df["weekday"] = df["datetime"].dt.dayofweek
    df["month"] = df["datetime"].dt.month
else:
    st.error("As colunas 'data' ou 'hora' não foram encontradas nos dados carregados.")
    st.stop()

# KPIs (Métricas principais)
if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    if "temperatura_ar" in df.columns:
        col1.metric("Temperatura Média", f'{(df["temperatura_ar"].mean()):.1f}°C')
    if "umidade_relativa" in df.columns:
        col2.metric("Umidade Relativa", f'{(df["umidade_relativa"].mean()):.1f}%')
    if "precipitacao_total" in df.columns:
        col3.metric("Precipitação Total", f'{(df["precipitacao_total"].sum()):.1f} mm')
    if "vento_velocidade" in df.columns:
        col4.metric("Velocidade do Vento", f'{(df["vento_velocidade"].mean()):.1f} m/s')

# Informações do modelo
if show_model_info:
    st.sidebar.subheader("Informações do Modelo")
    # ... (código do modelo permanece o mesmo)

# Previsões
if show_predictions and not df.empty:
    st.subheader("Previsão de Precipitação")
    # ... (código de previsões permanece o mesmo)

# Tabs para diferentes visualizações
# Agora este código pode acessar a coluna 'datetime' com segurança.
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Temperatura", "Umidade", "Precipitação", "Pressão", "Vento", "Radiação Solar", "Mapa de Calor"])

with tab1:
    st.plotly_chart(px.line(df, x="datetime", y="temperatura_ar", title="Variação de Temperatura", labels={"temperatura_ar": "Temperatura (°C)", "datetime": "Data e Hora"}), use_container_width=True)
with tab2:
    st.plotly_chart(px.line(df, x="datetime", y="umidade_relativa", title="Umidade Relativa", labels={"umidade_relativa": "Umidade (%)", "datetime": "Data e Hora"}), use_container_width=True)
with tab3:
    st.plotly_chart(px.bar(df, x="datetime", y="precipitacao_total", title="Precipitação", labels={"precipitacao_total": "Precipitação (mm)", "datetime": "Data e Hora"}), use_container_width=True)
with tab4:
    st.plotly_chart(px.line(df, x="datetime", y="pressao_atm_estacao", title="Pressão Atmosférica", labels={"pressao_atm_estacao": "Pressão (mB)", "datetime": "Data e Hora"}), use_container_width=True)
with tab5:
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(px.line(df, x="datetime", y="vento_velocidade", title="Velocidade do Vento", labels={"vento_velocidade": "Velocidade (m/s)", "datetime": "Data e Hora"}), use_container_width=True)
    with col2:
        wind_df = df.dropna(subset=["vento_direcao", "vento_velocidade"])
        if not wind_df.empty:
            st.plotly_chart(px.bar_polar(wind_df, r="vento_velocidade", theta="vento_direcao", color="vento_velocidade", template="plotly_dark", title="Rosa dos Ventos"), use_container_width=True)
with tab6:
    st.plotly_chart(px.line(df, x="datetime", y="radiacao_global", title="Radiação Solar", labels={"radiacao_global": "Radiação (W/m²)", "datetime": "Data e Hora"}), use_container_width=True)
with tab7:
    pivot_table = df.pivot_table(index="hour", columns=df["datetime"].dt.date, values="temperatura_ar", aggfunc="mean")
    st.plotly_chart(go.Figure(data=go.Heatmap(z=pivot_table.values, x=pivot_table.columns, y=pivot_table.index, colorscale="Viridis")), use_container_width=True)

# Mostrar dados brutos
if show_raw:
    st.subheader("Dados Brutos")
    st.dataframe(df)




