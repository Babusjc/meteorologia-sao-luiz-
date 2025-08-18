import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from scripts.database import NeonDB
from dotenv import load_dotenv
import joblib
import numpy as np
from datetime import datetime, timedelta

# Configuração
load_dotenv()
st.set_page_config(page_title="Dados Meteorológicos", layout="wide")
db = NeonDB()

# Título
st.title("🌦️ Dashboard Meteorológico - São Luiz do Paraitinga")
st.markdown("Dados da estação INMET A740 atualizados automaticamente")

# Filtros
st.sidebar.header("Filtros")
default_start = datetime.now() - timedelta(days=30)
date_range = st.sidebar.date_input("Período", [default_start.date(), datetime.now().date()])
show_raw = st.sidebar.checkbox("Mostrar dados brutos")
show_predictions = st.sidebar.checkbox("Mostrar previsões")

# Recuperar dados com cache
@st.cache_data(ttl=3600)
def load_data():
    return db.get_data("""
        SELECT 
            data, hora,
            precipitacao_total,
            pressao_atm_estacao,
            temperatura_ar,
            umidade_relativa,
            vento_velocidade,
            vento_direcao,
            radiacao_global,
            temperatura_max,
            temperatura_min
        FROM meteo_data
        ORDER BY data DESC, hora DESC
    """)

df = load_data()

# Processar filtros
if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    df = df[(df['data'] >= start_date) & (df['data'] <= end_date)]

# Criar coluna datetime combinada
df['datetime'] = pd.to_datetime(df['data'].astype(str) + ' ' + df['hora'].astype(str)

# KPIs
if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Temperatura Média", f"{df['temperatura_ar'].mean():.1f}°C")
    col2.metric("Umidade Relativa", f"{df['umidade_relativa'].mean():.1f}%")
    col3.metric("Precipitação Total", f"{df['precipitacao_total'].sum():.1f} mm")
    col4.metric("Velocidade do Vento", f"{df['vento_velocidade'].mean():.1f} m/s")

# Previsões
if show_predictions:
    st.subheader("Previsão de Precipitação")
    try:
        model = joblib.load('models/precipitation_model.pkl')
        
        # Criar dados de exemplo para previsão
        sample_data = {
            'temperatura_ar': [25.0],
            'umidade_relativa': [70.0],
            'pressao_atm_estacao': [1013.0],
            'radiacao_global': [500.0],
            'hour': [12],
            'day_of_week': [3],
            'month': [7]
        }
        sample_df = pd.DataFrame(sample_data)
        
        prediction = model.predict(sample_df)
        st.metric("Previsão de Precipitação", f"{prediction[0]:.1f} mm")
        
    except Exception as e:
        st.error(f"Erro ao carregar modelo: {str(e)}")

# Tabs para diferentes visualizações
tab1, tab2, tab3, tab4 = st.tabs(["Temperatura", "Umidade", "Precipitação", "Pressão"])

with tab1:
    if not df.empty:
        fig_temp = go.Figure()
        fig_temp.add_trace(go.Scatter(
            x=df['datetime'], 
            y=df['temperatura_ar'], 
            name='Temperatura',
            line=dict(color='red')
        ))
        fig_temp.add_trace(go.Scatter(
            x=df['datetime'], 
            y=df['temperatura_max'], 
            name='Máxima',
            line=dict(color='darkred', dash='dash')
        ))
        fig_temp.add_trace(go.Scatter(
            x=df['datetime'], 
            y=df['temperatura_min'], 
            name='Mínima',
            line=dict(color='blue', dash='dash')
        ))
        fig_temp.update_layout(
            title="Variação de Temperatura",
            yaxis_title="Temperatura (°C)",
            hovermode="x unified"
        )
        st.plotly_chart(fig_temp, use_container_width=True)
    else:
        st.write("Sem dados de temperatura")

with tab2:
    if not df.empty:
        fig_umidade = px.line(
            df, 
            x='datetime', 
            y='umidade_relativa', 
            title="Umidade Relativa",
            labels={'umidade_relativa': 'Umidade (%)'}
        )
        st.plotly_chart(fig_umidade, use_container_width=True)
    else:
        st.write("Sem dados de umidade")

with tab3:
    if not df.empty:
        fig_precip = px.bar(
            df, 
            x='datetime', 
            y='precipitacao_total', 
            title="Precipitação",
            labels={'precipitacao_total': 'Precipitação (mm)'}
        )
        st.plotly_chart(fig_precip, use_container_width=True)
    else:
        st.write("Sem dados de precipitação")

with tab4:
    if not df.empty:
        fig_pressao = px.line(
            df, 
            x='datetime', 
            y='pressao_atm_estacao', 
            title="Pressão Atmosférica",
            labels={'pressao_atm_estacao': 'Pressão (mB)'}
        )
        st.plotly_chart(fig_pressao, use_container_width=True)
    else:
        st.write("Sem dados de pressão")

# Dados brutos
if show_raw and not df.empty:
    st.subheader("Dados Brutos")
    st.dataframe(df.sort_values('datetime', ascending=False).head(100))
  
