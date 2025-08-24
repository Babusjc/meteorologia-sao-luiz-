import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV, TimeSeriesSplit
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, f1_score, roc_auc_score
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.compose import ColumnTransformer
from imblearn.over_sampling import SMOTE, ADASYN
from imblearn.under_sampling import RandomUnderSampler, TomekLinks
from imblearn.pipeline import make_pipeline as make_imb_pipeline
from imblearn.ensemble import BalancedRandomForestClassifier
import joblib
from pathlib import Path
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

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
        df['season'] = (df['month'] % 12 + 3) // 3  # 1:Inverno, 2:Primavera, 3:Verão, 4:Outono
    
    # Preparar dados para modelagem
    if "precipitacao_total" not in df.columns:
        logging.error("Dados de precipitação ausentes")
        return
    
    # Criar variável alvo (próxima hora) com groupby para evitar vazamento entre dias
    df["target"] = df.groupby('data')["precipitacao_total"].shift(-1).fillna(0)
    
    # Classificar chuva em categorias com thresholds ajustados
    conditions = [
        (df["target"] == 0),
        (df["target"] > 0) & (df["target"] <= 2.5),  # Chuva leve
        (df["target"] > 2.5)  # Chuva forte
    ]
    choices = [0, 1, 2]
    df["rain_class"] = np.select(conditions, choices, default=0)
    
    # Features básicas
    features = [
        "temperatura_ar", "umidade_relativa", "pressao_atm_estacao",
        "radiacao_global", "temperatura_max", "temperatura_min",
        "hour", "weekday", "month", "season"
    ]
    
    # Adicionar features de tendência e interações
    df["pressure_change_1h"] = df.groupby("data")["pressao_atm_estacao"].diff().fillna(0)
    df["pressure_change_3h"] = df.groupby("data")["pressao_atm_estacao"].diff(3).fillna(0)
    df["temp_change_1h"] = df.groupby("data")["temperatura_ar"].diff().fillna(0)
    df["temp_change_3h"] = df.groupby("data")["temperatura_ar"].diff(3).fillna(0)
    df["humidity_change_1h"] = df.groupby("data")["umidade_relativa"].diff().fillna(0)
    
    # CORREÇÃO: Calcular a média móvel da umidade (6 horas) de forma correta
    df["humidity_trend_6h"] = df.groupby("data")["umidade_relativa"].transform(
        lambda x: x.rolling(6, min_periods=1).mean()
    ).fillna(0)
    
    # Features de interação
    df["temp_humidity_interaction"] = df["temperatura_ar"] * df["umidade_relativa"]
    df["pressure_temp_interaction"] = df["pressao_atm_estacao"] * df["temperatura_ar"]
    
    # Adicionar todas as novas features à lista
    new_features = [
        "pressure_change_1h", "pressure_change_3h", "temp_change_1h", "temp_change_3h",
        "humidity_change_1h", "humidity_trend_6h", "temp_humidity_interaction", 
        "pressure_temp_interaction"
    ]
    
    features.extend(new_features)
    
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
    
    # Dividir dados com validação temporal
    tscv = TimeSeriesSplit(n_splits=5)
    
    # Configurar modelos e pipelines para testar
    models = {
        'BalancedRandomForest': BalancedRandomForestClassifier(
            n_estimators=100, 
            max_depth=15, 
            random_state=42,
            sampling_strategy='auto',
            n_jobs=-1
        ),
        'RandomForest': RandomForestClassifier(
            n_estimators=100,
            max_depth=15,
            class_weight='balanced',
            random_state=42,
            n_jobs=-1
        ),
        'GradientBoosting': GradientBoostingClassifier(
            n_estimators=100,
            max_depth=5,
            random_state=42
        )
    }
    
    best_score = 0
    best_model = None
    best_model_name = ""
    
    for name, model in models.items():
        logging.info(f"Testando modelo: {name}")
        
        # Avaliar com validação cruzada temporal
        try:
            cv_scores = cross_val_score(
                model, X, y, 
                cv=tscv, 
                scoring='f1_weighted',
                n_jobs=-1
            )
            mean_score = cv_scores.mean()
            logging.info(f"F1-Score médio ({name}): {mean_score:.3f}")
            
            if mean_score > best_score:
                best_score = mean_score
                best_model = model
                best_model_name = name
        except Exception as e:
            logging.error(f"Erro ao avaliar {name}: {str(e)}")
    
    if best_model is None:
        logging.error("Nenhum modelo pôde ser avaliado")
        return
    
    logging.info(f"Melhor modelo: {best_model_name} com F1-Score: {best_score:.3f}")
    
    # Treinar o melhor modelo em todos os dados
    best_model.fit(X, y)
    logging.info("Melhor modelo treinado com sucesso")
    
    # Fazer previsões em todo o conjunto para avaliação
    y_pred = best_model.predict(X)
    
    # Avaliar
    accuracy = accuracy_score(y, y_pred)
    f1 = f1_score(y, y_pred, average='weighted')
    
    logging.info(f"Acurácia: {accuracy:.3f}")
    logging.info(f"F1-Score (weighted): {f1:.3f}")
    logging.info("\nRelatório de Classificação:\n" + classification_report(y, y_pred))
    
    # Matriz de confusão
    cm = confusion_matrix(y, y_pred)
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
               xticklabels=['Sem Chuva', 'Chuva Leve', 'Chuva Forte'],
               yticklabels=['Sem Chuva', 'Chuva Leve', 'Chuva Forte'])
    plt.ylabel('Verdadeiro')
    plt.xlabel('Previsto')
    plt.title('Matriz de Confusão')
    
    # Salvar matriz de confusão
    model_dir = Path("models")
    model_dir.mkdir(exist_ok=True)
    plt.savefig(model_dir / "confusion_matrix.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # Salvar modelo
    joblib.dump(best_model, model_dir / "precipitation_model_improved.pkl")
    logging.info("Modelo melhorado salvo com sucesso")
    
    # Salvar também informações sobre as features
    if hasattr(best_model, 'feature_importances_'):
        feature_importance = pd.DataFrame({
            'feature': available_features,
            'importance': best_model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        feature_importance.to_csv(model_dir / "feature_importance.csv", index=False)
        logging.info("Importância das features salva")
        
        # Plotar importância das features
        plt.figure(figsize=(12, 8))
        sns.barplot(x='importance', y='feature', data=feature_importance.head(15))
        plt.title('Top 15 Features por Importância')
        plt.tight_layout()
        plt.savefig(model_dir / "feature_importance_plot.png", dpi=300, bbox_inches='tight')
        plt.close()
    
    # Testar técnicas de balanceamento para melhorar ainda mais
    logging.info("Testando técnicas de balanceamento...")
    
    try:
        # Amostrar aleatoriamente para reduzir o tamanho do dataset
        sample_size = min(30000, len(X))
        if len(X) > 30000:
            idx = np.random.choice(len(X), sample_size, replace=False)
            X_sample = X.iloc[idx]
            y_sample = y.iloc[idx]
            logging.info(f"Dataset reduzido para {sample_size} amostras para balanceamento")
        else:
            X_sample = X
            y_sample = y
        
        # Usar RandomUnderSampler para reduzir a classe majoritária primeiro
        rus = RandomUnderSampler(sampling_strategy={0: 50000}, random_state=42)
        X_res, y_res = rus.fit_resample(X_sample, y_sample)
        
        # Aplicar SMOTE apenas nas classes minoritárias
        smote = SMOTE(sampling_strategy={1: 8000, 2: 3000}, random_state=42)
        X_balanced, y_balanced = smote.fit_resample(X_res, y_res)
        
        # Treinar modelo nos dados balanceados
        balanced_model = best_model.__class__(**best_model.get_params())
        balanced_model.fit(X_balanced, y_balanced)
        
        # Avaliar
        y_pred_balanced = balanced_model.predict(X)
        f1_balanced = f1_score(y, y_pred_balanced, average='weighted')
        logging.info(f"F1-Score com balanceamento: {f1_balanced:.3f}")
        
        # Salvar se for melhor
        if f1_balanced > f1:
            joblib.dump(balanced_model, model_dir / "precipitation_model_balanced.pkl")
            logging.info("Modelo balanceado salvo com sucesso")

    except Exception as e:
        logging.error(f"Erro no balanceamento: {str(e)}")
        logging.info("Continuando sem balanceamento...")

if __name__ == "__main__":
    train_precipitation_model()
    
