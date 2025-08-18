import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import joblib
import numpy as np
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts.database import NeonDB
from dotenv import load_dotenv

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
@st.cache_data(ttl=3600, show_spinner="Carregando dados...")
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
if 'data' in df.columns and 'hora' in df.columns:
    df['datetime'] = pd.to_datetime(df['data'].astype(str) + ' ' + df['hora'].astype(str))

# KPIs
if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    if 'temperatura_ar' in df.columns:
        col1.metric("Temperatura Média", f"{df['temperatura_ar'].mean():.1f}°C")
    else:
        col1.metric("Temperatura", "Dados indisponíveis")
    
    if 'umidade_relativa' in df.columns:
        col2.metric("Umidade Relativa", f"{df['umidade_relativa'].mean():.1f}%")
    else:
        col2.metric("Umidade", "Dados indisponíveis")
    
    if 'precipitacao_total' in df.columns:
        col3.metric("Precipitação Total", f"{df['precipitacao_total'].sum():.1f} mm")
    else:
        col3.metric("Precipitação", "Dados indisponíveis")
    
    if 'vento_velocidade' in df.columns:
        col4.metric("Velocidade do Vento", f"{df['vento_velocidade'].mean():.1f} m/s")
    else:
        col4.metric("Vento", "Dados indisponíveis")

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
            'temperatura_max': [28.0],
            'temperatura_min': [22.0],
            'hour': [12],
            'day_of_week': [3],
            'month': [7]
        }
        sample_df = pd.DataFrame(sample_data)
        
        prediction = model.predict(sample_df)
        st.metric("Previsão de Precipitação", f"{prediction[0]:.1f} mm")
        st.info("Esta é uma previsão de exemplo com dados simulados")
        
    except Exception as e:
        st.error(f"Erro ao carregar modelo: {str(e)}")

# Tabs para diferentes visualizações
tab1, tab2, tab3, tab4 = st.tabs(["Temperatura", "Umidade", "Precipitação", "Pressão"])

with tab1:
    if not df.empty and 'temperatura_ar' in df.columns and 'datetime' in df.columns:
        fig_temp = go.Figure()
        fig_temp.add_trace(go.Scatter(
            x=df['datetime'], 
            y=df['temperatura_ar'], 
            name='Temperatura',
            line=dict(color='red')
        ))
        
        if 'temperatura_max' in df.columns:
            fig_temp.add_trace(go.Scatter(
                x=df['datetime'], 
                y=df['temperatura_max'], 
                name='Máxima',
                line=dict(color='darkred', dash='dash')
            ))
        
        if 'temperatura_min' in df.columns:
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
        st.warning("Dados de temperatura indisponíveis")

with tab2:
    if not df.empty and 'umidade_relativa' in df.columns and 'datetime' in df.columns:
        fig_umidade = px.line(
            df, 
            x='datetime', 
            y='umidade_relativa', 
            title="Umidade Relativa",
            labels={'umidade_relativa': 'Umidade (%)'}
        )
        st.plotly_chart(fig_umidade, use_container_width=True)
    else:
        st.warning("Dados de umidade indisponíveis")

with tab3:
    if not df.empty and 'precipitacao_total' in df.columns and 'datetime' in df.columns:
        fig_precip = px.bar(
            df, 
            x='datetime', 
            y='precipitacao_total', 
            title="Precipitação",
            labels={'precipitacao_total': 'Precipitação (mm)'}
        )
        st.plotly_chart(fig_precip, use_container_width=True)
    else:
        st.warning("Dados de precipitação indisponíveis")

with tab4:
    if not df.empty and 'pressao_atm_estacao' in df.columns and 'datetime' in df.columns:
        fig_pressao = px.line(
            df, 
            x='datetime', 
            y='pressao_atm_estacao', 
            title="Pressão Atmosférica",
            labels={'pressao_atm_estacao': 'Pressão (mB)'}
        )
        st.plotly_chart(fig_pressao, use_container_width=True)
    else:
        st.warning("Dados de pressão indisponíveis")

# Dados brutos
if show_raw and not df.empty:
    st.subheader("Dados Brutos")
    st.dataframe(df.sort_values('datetime' if 'datetime' in df.columns else 'data', ascending=False).head(100))
  
