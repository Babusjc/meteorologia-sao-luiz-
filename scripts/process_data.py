import pandas as pd
import numpy as np
import os
from pathlib import Path

def clean_and_transform():
    # Configurar caminhos
    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Listar arquivos CSV
    csv_files = list(raw_dir.glob("*.csv"))
    
    # Processar cada arquivo
    dfs = []
    for file in csv_files:
        try:
            # Ler com tratamento de erros
            df = pd.read_csv(
                file,
                sep=";",
                decimal=",",
                encoding="latin1",
                on_bad_lines="skip",
                na_values=["", " ", "null", "NaN"]
            )
            
            # Padronizar nomes de colunas
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            
            # Converter datas
            if "data" in df.columns and "hora" in df.columns:
                df["datetime"] = pd.to_datetime(
                    df["data"].astype(str) + " " + df["hora"].astype(str),
                    errors="coerce"
                )
            elif "data" in df.columns:
                df["datetime"] = pd.to_datetime(df["data"], errors="coerce")
            
            # Adicionar metadados temporais
            if "datetime" in df.columns:
                df["hour"] = df["datetime"].dt.hour
                df["weekday"] = df["datetime"].dt.dayofweek
                df["month"] = df["datetime"].dt.month
            
            # Selecionar colunas relevantes
            relevant_cols = [
                "datetime", "precipitacao_total", "pressao_atm_estacao",
                "temperatura_ar", "umidade_relativa", "vento_velocidade",
                "vento_direcao", "radiacao_global", "temperatura_max",
                "temperatura_min", "hour", "weekday", "month"
            ]
            
            # Filtrar colunas existentes
            available_cols = [col for col in relevant_cols if col in df.columns]
            df = df[available_cols]
            
            dfs.append(df)
            print(f"Processed {file.name} successfully")
            
        except Exception as e:
            print(f"Error processing {file.name}: {str(e)}")
    
    # Combinar todos os dados
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Remover duplicatas
        combined_df = combined_df.drop_duplicates(subset="datetime", keep="last")
        
        # Ordenar por data
        combined_df = combined_df.sort_values("datetime")
        
        # Salvar dados processados
        combined_df.to_parquet(processed_dir / "processed_weather_data.parquet", index=False)
        print(f"Saved processed data with {len(combined_df)} rows")
    else:
        print("No data processed")

if __name__ == "__main__":
    clean_and_transform()
