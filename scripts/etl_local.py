import pandas as pd
import numpy as np
import os
from pathlib import Path
import sys
from dotenv import load_dotenv
import logging
from collections import Counter

sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts.database import ETLDB

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_and_transform():
    load_dotenv()
    
    db = None
    try:
        db = ETLDB()
        logging.info("Conexão com o banco de dados estabelecida com sucesso para ETL")
    except Exception as e:
        logging.error(f"Falha ao conectar ao banco de dados para ETL: {str(e)}")
        return

    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    csv_files = list(raw_dir.glob("*.csv"))
    
    if not csv_files:
        logging.warning("Nenhum arquivo CSV encontrado para processamento")
        if db: db.close()
        return
    
    dfs = []
    for file in csv_files:
        try:
            logging.info(f"Iniciando processamento do arquivo: {file.name}")
            
            successful_read = False
            encoding_options = ['latin1', 'utf-8', 'iso-8859-1']
            separator_options = [';', '\t', ',']
            
            for encoding in encoding_options:
                for separator in separator_options:
                    try:
                        df = pd.read_csv(
                            file,
                            sep=separator,
                            decimal=",",
                            encoding=encoding,
                            on_bad_lines="skip",
                            na_values=["", " ", "null", "NaN", "null", "NULL"]
                        )
                        
                        if len(df) == 0:
                            logging.warning(f"Arquivo {file.name} está vazio")
                            continue
                            
                        if len(df.columns) < 2:
                            logging.warning(f"Arquivo {file.name} não tem colunas suficientes: {len(df.columns)}")
                            continue
                        
                        logging.info(f"Arquivo {file.name} lido com sucesso usando encoding={encoding}, separator='{separator}'")
                        successful_read = True
                        break
                    except Exception as e:
                        logging.debug(f"Tentativa falhou - encoding={encoding}, separator='{separator}': {str(e)}")
                        continue
                
                if successful_read:
                    break
            
            if not successful_read:
                logging.error(f"Falha ao ler arquivo {file.name} com qualquer combinação de encoding/separador")
                continue
            
            logging.info(f"Processando {file.name} - {len(df)} linhas iniciais")
            
            logging.info(f"Colunas encontradas: {list(df.columns)}")
            if len(df) > 0:
                logging.info(f"Primeira linha de dados: {dict(df.iloc[0])}")
            
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            logging.info(f"Colunas após padronização: {list(df.columns)}")
            
            column_mapping = {
                'data': 'data',
                'hora': 'hora',
                'precipitação_total': 'precipitacao_total',
                'precipitacao_total': 'precipitacao_total',
                'pressao_atmosferica_ao_nivel_da_estacao,_horaria_(mb)': 'pressao_atm_estacao',
                'pressao_atm_estacao': 'pressao_atm_estacao',
                'radiacao_global_(kj/m²)': 'radiacao_global',
                'radiacao_global': 'radiacao_global',
                'temperatura_do_ar_(°c)': 'temperatura_ar',
                'temperatura_ar': 'temperatura_ar',
                'umidade_relativa_do_ar,_horaria_(%)': 'umidade_relativa',
                'umidade_relativa': 'umidade_relativa',
                'vento_-_velocidade_horaria_(m/s)': 'vento_velocidade',
                'vento_velocidade': 'vento_velocidade',
                'vento_-_direção_horaria_(gr)': 'vento_direcao',
                'vento_direcao': 'vento_direcao',
                'temperatura_máxima_na_hora_ant._(°c)': 'temperatura_max',
                'temperatura_max': 'temperatura_max',
                'temperatura_mínima_na_hora_ant._(°c)': 'temperatura_min',
                'temperatura_min': 'temperatura_min'
            }
            
            existing_columns = {}
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    existing_columns[old_name] = new_name
            
            df = df.rename(columns=existing_columns)
            logging.info(f"Colunas após renomeação: {list(df.columns)}")
            
            relevant_cols = [
                "data", "hora", "precipitacao_total", "pressao_atm_estacao",
                "temperatura_ar", "umidade_relativa", "vento_velocidade",
                "vento_direcao", "radiacao_global", "temperatura_max", "temperatura_min"
            ]
            
            available_cols = [col for col in relevant_cols if col in df.columns]
            df = df[available_cols]
            
            if 'data' not in df.columns or 'hora' not in df.columns:
                logging.error(f"Colunas 'data' ou 'hora' não encontradas em {file.name}")
                logging.error(f"Colunas disponíveis: {list(df.columns)}")
                continue
            
            if 'data' in df.columns:
                df['data'] = pd.to_datetime(df['data'], errors='coerce', dayfirst=True).dt.date
            
            if 'hora' in df.columns:
                df['hora'] = pd.to_datetime(df['hora'], format='%H:%M', errors='coerce').dt.time
            
            numeric_cols = ['precipitacao_total', 'pressao_atm_estacao', 'temperatura_ar', 
                           'umidade_relativa', 'vento_velocidade', 'vento_direcao', 
                           'radiacao_global', 'temperatura_max', 'temperatura_min']
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logging.info(f"Antes da limpeza - {file.name}: {len(df)} linhas")
            
            initial_count = len(df)
            df = df.dropna(subset=['data', 'hora'])
            removed_count = initial_count - len(df)
            
            if removed_count > 0:
                logging.warning(f"Removidas {removed_count} linhas com data/hora inválidos em {file.name}")
            
            if len(df) > 0:
                logging.info(f"Após limpeza - {file.name}: {len(df)} linhas válidas")
                dfs.append(df)
            else:
                logging.warning(f"Arquivo {file.name} não contém dados válidos após limpeza")
            
        except Exception as e:
            logging.error(f"Erro ao processar {file.name}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        
        combined_df = combined_df.drop_duplicates(subset=["data", "hora"], keep="last")
        
        combined_df = combined_df.dropna(subset=['data', 'hora'])
        
        combined_df = combined_df.sort_values(["data", "hora"])
        
        combined_df["pressure_change"] = combined_df.groupby("data")["pressao_atm_estacao"].diff().fillna(0)
        combined_df["temp_change_3h"] = combined_df.groupby("data")["temperatura_ar"].diff(3).fillna(0)
        
        combined_df["humidity_trend"] = combined_df.groupby("data")["umidade_relativa"].transform(
            lambda x: x.rolling(6, min_periods=1).mean()
        ).fillna(0)
        
        combined_df.to_parquet(processed_dir / "processed_weather_data.parquet", index=False)
        logging.info(f"Dados processados salvos com {len(combined_df)} registros")
        
        try:
            db_cols = [col for col in relevant_cols if col in combined_df.columns]
            df_to_insert_db = combined_df[db_cols].copy()
            
            df_to_insert_db = df_to_insert_db.replace({pd.NaT: None, np.nan: None})
            
            initial_db_count = len(df_to_insert_db)
            df_to_insert_db = df_to_insert_db.dropna(subset=['data', 'hora'])
            final_db_count = len(df_to_insert_db)
            
            if final_db_count < initial_db_count:
                logging.warning(f"Removidas {initial_db_count - final_db_count} linhas com data/hora inválidos para inserção no banco")
            
            if df_to_insert_db.empty:
                logging.warning("Nenhum dado válido para inserção após limpeza de nulos.")
                # No need to ask user for confirmation, this is an internal process.
                # However, if this were a user-facing application, I would ask for confirmation.
            else:
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
        logging.warning("Nenhum dado processado para combinar")
    
    if db: 
        db.close()
        logging.info("Conexão com o banco de dados fechada")

if __name__ == "__main__":
    logging.info("Iniciando processo ETL")
    clean_and_transform()
    logging.info("Processo ETL concluído")
