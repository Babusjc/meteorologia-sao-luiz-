# --- INÍCIO DO BLOCO DE AJUSTE DE PATH --- #
# Este bloco DEVE ser o primeiro código a ser executado no arquivo.
# Ele ajusta o sys.path para que o Python possa encontrar módulos
# na pasta 'scripts/' que está na raiz do repositório.
#
# O 'dashboard.py' está em '.app/dashboard.py'.
# Para chegar à raiz do repositório, precisamos subir dois níveis:
# Path(__file__).parent (chega em .app/)
# .parent (chega na raiz do repositório)
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent

# Adiciona a raiz do projeto ao sys.path se ainda não estiver lá
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM DO BLOCO DE AJUSTE DE PATH --- #

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import joblib
import numpy as np
from datetime import datetime, timedelta
import calendar
from scripts.database import NeonDB # Agora esta importação funcionará
from dotenv import load_dotenv


# Configuração
load_dotenv()
st.set_page_config(page_title="Dados Meteorológicos", layout="wide")

# Inicializar conexão com banco de dados
@st.cache_resource
def init_db():
    try:
        db = NeonDB()
        return db
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {str(e)}")
        return None

db = init_db()

# Título
st.title("🌦️ Dashboard Meteorológico - São Luiz do Paraitinga")
st.markdown("Dados da estação INMET A740 atualizados automaticamente")

# Filtros
st.sidebar.header("Filtros")
default_start = datetime.now() - timedelta(days=30)
date_range = st.sidebar.date_input("Período", [default_start.date(), datetime.now().date()])
show_raw = st.sidebar.checkbox("Mostrar dados brutos")
show_predictions = st.sidebar.checkbox("Mostrar previsões")
show_model_info = st.sidebar.checkbox("Mostrar informações do modelo")

# Recuperar dados com cache
@st.cache_data(ttl=3600, show_spinner="Carregando dados...")
def load_data():
    if db is None:
        return pd.DataFrame()
    
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
            temperatura_min,
            pressure_change,
            temp_change_3h,
            humidity_trend
        FROM meteo_data
        ORDER BY data DESC, hora DESC
    """)

df = load_data()

# Processar filtros
if len(date_range) == 2 and not df.empty:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    df = df[(df["data"] >= start_date.date()) & (df["data"] <= end_date.date())]

# Criar coluna datetime combinada
if not df.empty and "data" in df.columns and "hora" in df.columns:
    df["datetime"] = pd.to_datetime(df["data"].astype(str) + " " + df["hora"].astype(str))
    df["hour"] = df["datetime"].dt.hour
    df["weekday"] = df["datetime"].dt.dayofweek
    df["month"] = df["datetime"].dt.month

# KPIs
if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    if "temperatura_ar" in df.columns:
        col1.metric("Temperatura Média", f"{df["temperatura_ar"].mean():.1f}°C")
    else:
        col1.metric("Temperatura", "Dados indisponíveis")
    
    if "umidade_relativa" in df.columns:
        col2.metric("Umidade Relativa", f"{df["umidade_relativa"].mean():.1f}%")
    else:
        col2.metric("Umidade", "Dados indisponíveis")
    
    if "precipitacao_total" in df.columns:
        col3.metric("Precipitação Total", f"{df["precipitacao_total"].sum():.1f} mm")
    else:
        col3.metric("Precipitação", "Dados indisponíveis")
    
    if "vento_velocidade" in df.columns:
        col4.metric("Velocidade do Vento", f"{df["vento_velocidade"].mean():.1f} m/s")
    else:
        col4.metric("Vento", "Dados indisponíveis")

# Informações do modelo
if show_model_info:
    st.sidebar.subheader("Informações do Modelo")
    
    try:
        # Carregar importância das features
        feature_importance = pd.read_csv("models/feature_importance.csv")
        st.sidebar.write("**Importância das Features:**")
        for _, row in feature_importance.head(5).iterrows():
            st.sidebar.write(f"{row["feature"]}: {row["importance"]:.3f}")
    except:
        st.sidebar.info("Importância das features não disponível")
    
    try:
        # Mostrar matriz de confusão
        st.sidebar.image("models/confusion_matrix.png", caption="Matriz de Confusão")
    except:
        st.sidebar.info("Matriz de confusão não disponível")

# Previsões
if show_predictions and not df.empty:
    st.subheader("Previsão de Precipitação")
    
    try:
        # Tentar carregar o modelo melhorado primeiro
        try:
            model = joblib.load("models/precipitation_model_improved.pkl")
            model_type = "melhorado"
        except:
            # Fallback para o modelo antigo
            model = joblib.load("models/precipitation_model.pkl")
            model_type = "original"
        
        st.info(f"Usando modelo {model_type}")
        
        # Usar os últimos dados disponíveis para previsão
        last_entry = df.iloc[0]
        
        # Preparar dados para previsão incluindo as novas features
        prediction_data = {
            "temperatura_ar": [last_entry["temperatura_ar"]],
            "umidade_relativa": [last_entry["umidade_relativa"]],
            "pressao_atm_estacao": [last_entry["pressao_atm_estacao"]],
            "radiacao_global": [last_entry["radiacao_global"]],
            "temperatura_max": [last_entry["temperatura_max"]],
            "temperatura_min": [last_entry["temperatura_min"]],
            "hour": [last_entry["hour"]],
            "weekday": [last_entry["weekday"]],
            "month": [last_entry["month"]],
            "pressure_change": [last_entry.get("pressure_change", 0)],
            "temp_change_3h": [last_entry.get("temp_change_3h", 0)],
            "humidity_trend": [last_entry.get("humidity_trend", 0)]
        }
        
        # Remover features que não existem no modelo
        available_features = [f for f in prediction_data.keys() if f in model.feature_names_in_]
        filtered_prediction_data = {f: prediction_data[f] for f in available_features}
        
        prediction_df = pd.DataFrame(filtered_prediction_data)
        
        prediction = model.predict(prediction_df)
        
        # Mapear classes para rótulos
        class_labels = {
            0: "Sem Chuva",
            1: "Chuva Leve (até 5mm)",
            2: "Chuva Forte (>5mm)"
        }
        
        col1, col2 = st.columns(2)
        col1.metric("Previsão de Precipitação (próxima hora)", class_labels[prediction[0]])
        
        # Adicionar informações contextuais
        col2.markdown(f"**Dados usados para previsão:**")
        col2.markdown(f"- Temperatura: {last_entry["temperatura_ar"]:.1f}°C")
        col2.markdown(f"- Umidade: {last_entry["umidade_relativa"]:.1f}%")
        col2.markdown(f"- Pressão: {last_entry["pressao_atm_estacao"]:.1f} mB")
        col2.markdown(f"- Hora: {last_entry["hour"]}h")
        
        # Mapa de probabilidade
        try:
            proba = model.predict_proba(prediction_df)[0]
            proba_df = pd.DataFrame({
                "Probabilidade": proba,
                "Classe": [class_labels[i] for i in range(len(proba))]
            })
            
            fig_proba = px.bar(
                proba_df, 
                x="Classe", 
                y="Probabilidade",
                title="Probabilidade de Precipitação",
                text="Probabilidade",
                color="Classe",
                color_discrete_sequence=["green", "orange", "red"]
            )
            fig_proba.update_traces(texttemplate=\\'%{text:.0%}\\', textposition=\\'outside\\")
            fig_proba.update_layout(yaxis_tickformat=\\''.0%\\")
            st.plotly_chart(fig_proba, use_container_width=True)
        except Exception as e:
            st.info("Probabilidades não disponíveis para este modelo")
            st.error(str(e))
        
    except Exception as e:
        st.error(f"Erro ao carregar modelo: {str(e)}")
        st.info("Certifique-se que o arquivo do modelo está em \\'models/precipitation_model_improved.pkl\'")

# Tabs para diferentes visualizações
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Temperatura", "Umidade", "Precipitação", "Pressão", 
    "Vento", "Radiação Solar", "Mapa de Calor"
])

with tab1:
    if not df.empty and "temperatura_ar" in df.columns and "datetime" in df.columns:
        fig_temp = go.Figure()
        fig_temp.add_trace(go.Scatter(
            x=df["datetime"], 
            y=df["temperatura_ar"], 
            name="Temperatura",
            line=dict(color="red")
        ))
        
        if "temperatura_max" in df.columns:
            fig_temp.add_trace(go.Scatter(
                x=df["datetime"], 
                y=df["temperatura_max"], 
                name="Máxima",
                line=dict(color="darkred", dash="dash")
            ))
        
        if "temperatura_min" in df.columns:
            fig_temp.add_trace(go.Scatter(
                x=df["datetime"], 
                y=df["temperatura_min"], 
                name="Mínima",
                line=dict(color="blue", dash="dash")
            ))
        
        fig_temp.update_layout(
            title="Variação de Temperatura",
            yaxis_title="Temperatura (°C)",
            hovermode="x unified",
            xaxis_title="Data e Hora"
        )
        st.plotly_chart(fig_temp, use_container_width=True)
    else:
        st.warning("Dados de temperatura indisponíveis")

with tab2:
    if not df.empty and "umidade_relativa" in df.columns and "datetime" in df.columns:
        fig_umidade = px.line(
            df, 
            x="datetime", 
            y="umidade_relativa", 
            title="Umidade Relativa",
            labels={"umidade_relativa": "Umidade (%)"},
            color_discrete_sequence=["blue"]
        )
        fig_umidade.update_layout(xaxis_title="Data e Hora")
        st.plotly_chart(fig_umidade, use_container_width=True)
    else:
        st.warning("Dados de umidade indisponíveis")

with tab3:
    if not df.empty and "precipitacao_total" in df.columns and "datetime" in df.columns:
        fig_precip = px.bar(
            df, 
            x="datetime", 
            y="precipitacao_total", 
            title="Precipitação",
            labels={"precipitacao_total": "Precipitação (mm)"},
            color_discrete_sequence=["#1f77b4"]
        )
        fig_precip.update_layout(xaxis_title="Data e Hora")
        st.plotly_chart(fig_precip, use_container_width=True)
    else:
        st.warning("Dados de precipitação indisponíveis")

with tab4:
    if not df.empty and "pressao_atm_estacao" in df.columns and "datetime" in df.columns:
        fig_pressao = px.line(
            df, 
            x="datetime", 
            y="pressao_atm_estacao", 
            title="Pressão Atmosférica",
            labels={"pressao_atm_estacao": "Pressão (mB)"},
            color_discrete_sequence=["green"]
        )
        fig_pressao.update_layout(xaxis_title="Data e Hora")
        st.plotly_chart(fig_pressao, use_container_width=True)
        
        # Mostrar mudança de pressão se disponível
        if "pressure_change" in df.columns:
            fig_pressure_change = px.line(
                df, 
                x="datetime", 
                y="pressure_change", 
                title="Mudança de Pressão",
                labels={"pressure_change": "Δ Pressão (mB)"},
                color_discrete_sequence=["purple"]
            )
            fig_pressure_change.update_layout(xaxis_title="Data e Hora")
            st.plotly_chart(fig_pressure_change, use_container_width=True)
    else:
        st.warning("Dados de pressão indisponíveis")

with tab5:
    if not df.empty and "vento_velocidade" in df.columns and "vento_direcao" in df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_vento = px.line(
                df, 
                x="datetime", 
                y="vento_velocidade", 
                title="Velocidade do Vento",
                labels={"vento_velocidade": "Velocidade (m/s)"},
                color_discrete_sequence=["purple"]
            )
            fig_vento.update_layout(xaxis_title="Data e Hora")
            st.plotly_chart(fig_vento, use_container_width=True)
        
        with col2:
            # Rosa dos Ventos
            wind_df = df.dropna(subset=["vento_direcao", "vento_velocidade"])
            if not wind_df.empty:
                fig_rosa = px.bar_polar(
                    wind_df,
                    r="vento_velocidade",
                    theta="vento_direcao",
                    color="vento_velocidade",
                    template="plotly_dark",
                    color_continuous_scale=px.colors.sequential.Plasma_r,
                    title="Rosa dos Ventos"
                )
                st.plotly_chart(fig_rosa, use_container_width=True)
            else:
                st.warning("Dados insuficientes para gerar rosa dos ventos")
    else:
        st.warning("Dados de vento indisponíveis")

with tab6:
    if not df.empty and "radiacao_global" in df.columns and "datetime" in df.columns:
        fig_rad = px.line(
            df, 
            x="datetime", 
            y="radiacao_global", 
            title="Radiação Solar",
            labels={"radiacao_global": "Radiação (W/m²)"},
            color_discrete_sequence=["orange"]
        )
        fig_rad.update_layout(xaxis_title="Data e Hora")
        st.plotly_chart(fig_rad, use_container_width=True)
    else:
        st.warning("Dados de radiação solar indisponíveis")

with tab7:
    if not df.empty and "temperatura_ar" in df.columns and "datetime" in df.columns:
        # Preparar dados para heatmap
        df_heatmap = df.copy()
        df_heatmap["hour"] = df_heatmap["datetime"].dt.hour
        df_heatmap["date"] = df_heatmap["datetime"].dt.date
        
        pivot_table = df_heatmap.pivot_table(
            index="hour", 
            columns="date", 
            values="temperatura_ar", 
            aggfunc="mean"
        )
        
        fig_heatmap = go.Figure(data=go.Heatmap(
            z=pivot_table.values,
            x=pivot_table.columns.astype(str),
            y=pivot_table.index,
            colorscale="Viridis",
      
(Content truncated due to size limit. Use page ranges or line ranges to read remaining content)
