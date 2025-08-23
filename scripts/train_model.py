import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, f1_score
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import make_pipeline as make_imb_pipeline
import joblib
from pathlib import Path
import logging
import matplotlib.pyplot as plt
import seaborn as sns

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def train_precipitation_model():
    # Carregar dados processados
    data_path = Path("data/processed/processed_weather_data.parquet")
    if not data_path.exists():
        logging.error("Dados processados não encontrados")
        return
    
    try:
        df = pd.read_parquet(data_path)
        logging.info(f"Dados carregados com {len(df)} registros")
    except Exception as e:
        logging.error(f"Erro ao carregar dados: {str(e)}")
        return
    
    # Ordenar por data/hora para evitar vazamento temporal
    if 'data' in df.columns and 'hora' in df.columns:
        df['datetime'] = pd.to_datetime(df['data'].astype(str) + ' ' + df['hora'].astype(str))
        df = df.sort_values('datetime').reset_index(drop=True)
        df['hour'] = df['datetime'].dt.hour
        df['weekday'] = df['datetime'].dt.dayofweek
        df['month'] = df['datetime'].dt.month
    
    # Preparar dados para modelagem
    if "precipitacao_total" not in df.columns:
        logging.error("Dados de precipitação ausentes")
        return
    
    # Criar variável alvo (próxima hora) com groupby para evitar vazamento entre dias
    df["target"] = df.groupby('data')["precipitacao_total"].shift(-1).fillna(0)
    
    # Classificar chuva em categorias com thresholds ajustados
    conditions = [
        (df["target"] == 0),
        (df["target"] > 0) & (df["target"] <= 5.0),  # Aumentado para 5mm
        (df["target"] > 5.0)
    ]
    choices = [0, 1, 2]  # 0: Sem chuva, 1: Chuva leve, 2: Chuva forte
    df["rain_class"] = np.select(conditions, choices, default=0)
    
    # Features
    features = [
        "temperatura_ar", "umidade_relativa", "pressao_atm_estacao",
        "radiacao_global", "temperatura_max", "temperatura_min",
        "hour", "weekday", "month"
    ]
    
    # Adicionar features de tendência
    df["pressure_change"] = df["pressao_atm_estacao"].diff().fillna(0)
    df["temp_change_3h"] = df["temperatura_ar"].diff(3).fillna(0)
    df["humidity_trend"] = df["umidade_relativa"].rolling(6, min_periods=1).mean().fillna(0)
    
    features.extend(["pressure_change", "temp_change_3h", "humidity_trend"])
    
    # Filtrar apenas colunas disponíveis
    available_features = [f for f in features if f in df.columns]
    
    # Filtrar dados completos
    df = df.dropna(subset=available_features + ["rain_class"])
    
    if df.empty:
        logging.error("Nenhum dado disponível para treinamento")
        return
    
    X = df[available_features]
    y = df["rain_class"]
    
    # Verificar distribuição das classes
    class_distribution = y.value_counts()
    logging.info(f"Distribuição das classes: {dict(class_distribution)}")
    
    # Verificar se temos dados suficientes para treinamento
    if len(X) < 100:
        logging.error(f"Dados insuficientes para treinamento: apenas {len(X)} amostras")
        return
    
    # Dividir dados
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    logging.info(f"Treinando modelo com {len(X_train)} amostras")
    
    # Estratégia de balanceamento corrigida
    oversample = SMOTE(sampling_strategy='auto', random_state=42)
    undersample = RandomUnderSampler(sampling_strategy='auto', random_state=42)
    
    # Criar e treinar modelo com balanceamento
    try:
        # Pipeline com balanceamento e classificação
        model = make_imb_pipeline(
            undersample,
            oversample,
            StandardScaler(),
            RandomForestClassifier(
                n_estimators=100,
                max_depth=15,
                class_weight="balanced",
                random_state=42,
                n_jobs=-1  # Usar todos os cores disponíveis
            )
        )
        
        # Treinar modelo
        model.fit(X_train, y_train)
        logging.info("Modelo treinado com sucesso")
        
        # Fazer previsões
        y_pred = model.predict(X_test)
        
        # Avaliar
        accuracy = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average='weighted')
        
        logging.info(f"Acurácia: {accuracy:.3f}")
        logging.info(f"F1-Score (weighted): {f1:.3f}")
        logging.info("\nRelatório de Classificação:\n" + classification_report(y_test, y_pred))
        
        # Matriz de confusão
        cm = confusion_matrix(y_test, y_pred)
        plt.figure(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                   xticklabels=['Sem Chuva', 'Chuva Leve', 'Chuva Forte'],
                   yticklabels=['Sem Chuva', 'Chuva Leve', 'Chuva Forte'])
        plt.ylabel('Verdadeiro')
        plt.xlabel('Previsto')
        plt.title('Matriz de Confusão')
        
        # Salvar matriz de confusão
        model_dir = Path("models")
        model_dir.mkdir(exist_ok=True)
        plt.savefig(model_dir / "confusion_matrix.png")
        plt.close()
        
        # Salvar modelo
        joblib.dump(model, model_dir / "precipitation_model_improved.pkl")
        logging.info("Modelo melhorado salvo com sucesso")
        
        # Salvar também informações sobre as features
        feature_importance = pd.DataFrame({
            'feature': available_features,
            'importance': model.steps[-1][-1].feature_importances_
        }).sort_values('importance', ascending=False)
        
        feature_importance.to_csv(model_dir / "feature_importance.csv", index=False)
        logging.info("Importância das features salva")
        
    except Exception as e:
        logging.error(f"Erro ao treinar modelo: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    train_precipitation_model()



