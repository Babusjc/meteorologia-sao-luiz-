# /app/dashboard.py

# --- INÍCIO DO BLOCO DE AJUSTE DE PATH --- #
# Este bloco DEVE ser o primeiro código a ser executado no arquivo.
# Ele ajusta o sys.path para que o Python possa encontrar módulos
# na pasta 'scripts/' que está na raiz do repositório.
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

# ALTERAÇÃO: A classe NeonDB não é mais importada. Em vez disso,
# importamos a nova função que usa o sistema de conexão do Streamlit.
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

# ALTERAÇÃO: A função load_data foi simplificada para usar o novo método.
# O cache do Streamlit continua funcionando da mesma forma.
@st.cache_data(ttl=3600, show_spinner="Carregando dados...")
def load_data():
    """Busca os dados do banco de dados usando a nova função."""
    query = """
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
    """
    return get_data_from_db(query)

# Carrega os dados
df = load_data()

# Validação para interromper a execução se os dados não forem carregados
if df.empty:
    st.error("Não foi possível carregar os dados do banco de dados. Verifique as configurações de conexão nos 'Secrets' do Streamlit e se a tabela 'meteo_data' contém dados.")
    st.stop()

# Processar filtros de data
if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    df['data'] = pd.to_datetime(df['data'])
    df = df[(df["data"].dt.date >= start_date.date()) & (df["data"].dt.date <= end_date.date())]

# Criar colunas de data e hora para análise
if not df.empty:
    df["datetime"] = pd.to_datetime(df["data"].astype(str) + " " + df["hora"].astype(str))
    df["hour"] = df["datetime"].dt.hour
    df["weekday"] = df["datetime"].dt.dayofweek
    df["month"] = df["datetime"].dt.month

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

# O restante do seu script permanece exatamente o mesmo.
# Copie e cole daqui para baixo sem alterações.

# Informações do modelo
if show_model_info:
    st.sidebar.subheader("Informações do Modelo")
    try:
        feature_importance = pd.read_csv("models/feature_importance.csv")
        st.sidebar.write("**Importância das Features:**")
        for _, row in feature_importance.head(5).iterrows():
            st.sidebar.write(f"{row['feature']}: {row['importance']:.3f}")
    except FileNotFoundError:
        st.sidebar.info("Arquivo de importância das features não encontrado.")
    except Exception as e:
        st.sidebar.error(f"Erro ao ler features: {e}")

    try:
        st.sidebar.image("models/confusion_matrix.png", caption="Matriz de Confusão")
    except FileNotFoundError:
        st.sidebar.info("Imagem da matriz de confusão não encontrada.")

# Previsões
if show_predictions and not df.empty:
    st.subheader("Previsão de Precipitação")
    try:
        try:
            model = joblib.load("models/precipitation_model_improved.pkl")
            model_type = "melhorado"
        except FileNotFoundError:
            model = joblib.load("models/precipitation_model.pkl")
            model_type = "original"
        
        st.info(f"Usando modelo {model_type}")
        last_entry = df.iloc[0]
        
        prediction_data = {
            "temperatura_ar": [last_entry["temperatura_ar"]], "umidade_relativa": [last_entry["umidade_relativa"]],
            "pressao_atm_estacao": [last_entry["pressao_atm_estacao"]], "radiacao_global": [last_entry["radiacao_global"]],
            "temperatura_max": [last_entry["temperatura_max"]], "temperatura_min": [last_entry["temperatura_min"]],
            "hour": [last_entry["hour"]], "weekday": [last_entry["weekday"]], "month": [last_entry["month"]],
            "pressure_change": [last_entry.get("pressure_change", 0)], "temp_change_3h": [last_entry.get("temp_change_3h", 0)],
            "humidity_trend": [last_entry.get("humidity_trend", 0)]
        }
        
        available_features = [f for f in prediction_data.keys() if f in model.feature_names_in_]
        filtered_prediction_data = {f: prediction_data[f] for f in available_features}
        prediction_df = pd.DataFrame(filtered_prediction_data)
        prediction = model.predict(prediction_df)
        
        class_labels = {0: "Sem Chuva", 1: "Chuva Leve (até 5mm)", 2: "Chuva Forte (>5mm)"}
        
        col1, col2 = st.columns(2)
        col1.metric("Previsão de Precipitação (próxima hora)", class_labels[prediction[0]])
        
        col2.markdown(f"**Dados usados para previsão:**")
        col2.markdown(f"- Temperatura: {(last_entry['temperatura_ar']):.1f}°C")
        col2.markdown(f"- Umidade: {(last_entry['umidade_relativa']):.1f}%")
        col2.markdown(f"- Pressão: {(last_entry['pressao_atm_estacao']):.1f} mB")
        col2.markdown(f"- Hora: {last_entry['hour']}h")
        
        if hasattr(model, 'predict_proba'):
            proba = model.predict_proba(prediction_df)[0]
            proba_df = pd.DataFrame({"Probabilidade": proba, "Classe": [class_labels[i] for i in range(len(proba))]})
            fig_proba = px.bar(proba_df, x="Classe", y="Probabilidade", title="Probabilidade de Precipitação", text="Probabilidade", color="Classe", color_discrete_sequence=["green", "orange", "red"])
            fig_proba.update_traces(texttemplate='%{text:.0%}', textposition='outside')
            fig_proba.update_layout(yaxis_tickformat='.0%')
            st.plotly_chart(fig_proba, use_container_width=True)
        
    except FileNotFoundError:
        st.error("Arquivo do modelo de previsão não encontrado. Verifique se 'models/precipitation_model.pkl' ou 'models/precipitation_model_improved.pkl' existe.")
    except Exception as e:
        st.error(f"Erro ao carregar ou usar o modelo: {e}")

# Tabs para diferentes visualizações
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


