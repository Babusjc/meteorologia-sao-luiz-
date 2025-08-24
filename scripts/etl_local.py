import pandas as pd
import numpy as np
import os
from pathlib import Path
import sys
from dotenv import load_dotenv
import logging

# Adicionar o diretório pai ao path para importar módulos
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Importar a nova classe ETLDB para uso em scripts fora do Streamlit
from scripts.database import ETLDB

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_and_transform():
    # Carregar variáveis de ambiente (para NEON_DB_URL)
    load_dotenv()
    
    db = None
    try:
        # Conectar ao banco de dados usando a classe ETLDB
        db = ETLDB()
        logging.info("Conexão com o banco de dados estabelecida com sucesso para ETL")
    except Exception as e:
        logging.error(f"Falha ao conectar ao banco de dados para ETL: {str(e)}")
        return

    # Configurar caminhos
    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Listar arquivos CSV
    csv_files = list(raw_dir.glob("*.csv"))
    
    if not csv_files:
        logging.warning("Nenhum arquivo CSV encontrado para processamento")
        if db: db.close()
        return
    
    # Processar cada arquivo
    dfs = []
    for file in csv_files:
        try:
            # Ler arquivo CSV
            df = pd.read_csv(
                file,
                sep=";",
                decimal=",",
                encoding="latin1",
                on_bad_lines="skip",
                na_values=["", " ", "null", "NaN", "null", "NULL"]
            )
            
            # Log inicial do arquivo
            logging.info(f"Processando {file.name} - {len(df)} linhas iniciais")
            
            # Padronizar nomes de colunas
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            
            # Renomear colunas para corresponder ao schema do banco
            column_mapping = {
                'data': 'data',
                'horas': 'hora',
                'precipitação_total': 'precipitacao_total',
                'pressao_atmosferica_ao_nivel_da_estacao,_horaria_(mb)': 'pressao_atm_estacao',
                'radiacao_global_(kj/m²)': 'radiacao_global',
                'temperatura_do_ar_(°c)': 'temperatura_ar',
                'umidade_relativa_do_ar,_horaria_(%)': 'umidade_relativa',
                'vento_-_velocidade_horaria_(m/s)': 'vento_velocidade',
                'vento_-_direção_horaria_(gr)': 'vento_direcao',
                'temperatura_máxima_na_hora_ant._(°c)': 'temperatura_max',
                'temperatura_mínima_na_hora_ant._(°c)': 'temperatura_min'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Manter apenas colunas relevantes para o banco de dados
            relevant_cols = [
                "data", "hora", "precipitacao_total", "pressao_atm_estacao",
                "temperatura_ar", "umidade_relativa", "vento_velocidade",
                "vento_direcao", "radiacao_global", "temperatura_max", "temperatura_min"
            ]
            
            # Filtrar colunas existentes no DataFrame atual
            available_cols = [col for col in relevant_cols if col in df.columns]
            df = df[available_cols]
            
            # Converter tipos de dados
            if 'data' in df.columns:
                df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.date
            
            if 'hora' in df.columns:
                # Converter hora para formato time, tratando valores inválidos
                df['hora'] = pd.to_datetime(df['hora'], format='%H:%M', errors='coerce').dt.time
            
            # Converter colunas numéricas
            numeric_cols = ['precipitacao_total', 'pressao_atm_estacao', 'temperatura_ar', 
                           'umidade_relativa', 'vento_velocidade', 'vento_direcao', 
                           'radiacao_global', 'temperatura_max', 'temperatura_min']
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Verificar dados antes da limpeza
            logging.info(f"Antes da limpeza - {file.name}: {len(df)} linhas")
            
            # Remover linhas com dados essenciais faltantes
            initial_count = len(df)
            df = df.dropna(subset=['data', 'hora'])
            removed_count = initial_count - len(df)
            
            if removed_count > 0:
                logging.warning(f"Removidas {removed_count} linhas com data/hora inválidos em {file.name}")
            
            # Verificar dados após a limpeza
            if len(df) > 0:
                logging.info(f"Após limpeza - {file.name}: {len(df)} linhas válidas")
                dfs.append(df)
            else:
                logging.warning(f"Arquivo {file.name} não contém dados válidos após limpeza")
            
        except Exception as e:
            logging.error(f"Erro ao processar {file.name}: {str(e)}")
    
    # Combinar todos os dados
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Remover duplicatas
        combined_df = combined_df.drop_duplicates(subset=["data", "hora"], keep="last")
        
        # VERIFICAÇÃO CRÍTICA: Remover quaisquer linhas com valores nulos em data ou hora
        combined_df = combined_df.dropna(subset=['data', 'hora'])
        
        # Ordenar por data e hora
        combined_df = combined_df.sort_values(["data", "hora"])
        
        # Adicionar features de tendência para uso no modelo (estas não são salvas no banco)
        combined_df["pressure_change"] = combined_df.groupby("data")["pressao_atm_estacao"].diff().fillna(0)
        combined_df["temp_change_3h"] = combined_df.groupby("data")["temperatura_ar"].diff(3).fillna(0)
        
        # Calcular média móvel da umidade (6 horas)
        combined_df["humidity_trend"] = combined_df.groupby("data")["umidade_relativa"].transform(
            lambda x: x.rolling(6, min_periods=1).mean()
        ).fillna(0)
        
        # Salvar dados processados (para o modelo de ML)
        combined_df.to_parquet(processed_dir / "processed_weather_data.parquet", index=False)
        logging.info(f"Dados processados salvos com {len(combined_df)} registros")
        
        # Inserir no banco de dados (apenas colunas que existem no banco)
        try:
            # Filtrar o DataFrame para incluir apenas as colunas que serão inseridas no banco
            db_cols = [col for col in relevant_cols if col in combined_df.columns]
            df_to_insert_db = combined_df[db_cols].copy()
            
            # Garantir que não há valores NaT/NaN antes da inserção
            df_to_insert_db = df_to_insert_db.replace({pd.NaT: None, np.nan: None})
            
            # VERIFICAÇÃO FINAL: Remover quaisquer linhas com valores nulos nas colunas críticas
            initial_db_count = len(df_to_insert_db)
            df_to_insert_db = df_to_insert_db.dropna(subset=['data', 'hora'])
            final_db_count = len(df_to_insert_db)
            
            if final_db_count < initial_db_count:
                logging.warning(f"Removidas {initial_db_count - final_db_count} linhas com valores nulos para inserção no banco")
            
            if df_to_insert_db.empty:
                logging.error("Nenhum dado válido para inserção no banco após todas as limpezas")
                return
            
            logging.info(f"Preparando para inserir {len(df_to_insert_db)} registros no banco")
            
            success = db.insert_data(df_to_insert_db, "meteo_data")
            if success:
                logging.info(f"Dados inseridos no banco com sucesso: {len(df_to_insert_db)} registros")
            else:
                logging.error("Falha ao inserir dados no banco")
        except Exception as e:
            logging.error(f"Erro ao inserir dados no banco: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
    else:
        logging.warning("Nenhum dado processado")
    
    if db: db.close()

if __name__ == "__main__":
    clean_and_transform()
