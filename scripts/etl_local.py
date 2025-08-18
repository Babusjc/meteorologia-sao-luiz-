import os
import pandas as pd
import numpy as np
from pathlib import Path
from scripts.database import NeonDB
from dotenv import load_dotenv
import logging
import re

# Configuração
load_dotenv()
RAW_DATA_DIR = Path("C:/Users/josesantos/Downloads/dados")
PROCESSED_DIR = Path("data/processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_value(x):
    """Converte valores problemáticos para NaN e substitui vírgula por ponto"""
    if isinstance(x, str):
        x = x.strip()
        if x in ['', '-9999']:
            return np.nan
        # Substitui vírgula por ponto se for um número com vírgula decimal
        x = re.sub(r'(\d),(\d)', r'\1.\2', x)
    return x

def process_file(file_path: Path) -> pd.DataFrame:
    """Processa um único arquivo de dados"""
    try:
        # Lê o CSV com encoding latin-1 e delimitador ; e pula a primeira linha (cabeçalho)
        df = pd.read_csv(file_path, encoding='latin-1', delimiter=';', skiprows=1, low_memory=False)
        
        # Define os cabeçalhos conforme especificado
        columns = [
            "data", "horas", "precipitacao_total", "pressao_atm_estacao", 
            "pressao_atm_max", "pressao_atm_min", "radiacao_global", 
            "temperatura_ar", "temperatura_orvalho", "temperatura_max", 
            "temperatura_min", "temperatura_orvalho_max", "temperatura_orvalho_min",
            "umidade_rel_max", "umidade_rel_min", "umidade_relativa",
            "vento_direcao", "vento_rajada_max", "vento_velocidade"
        ]
        df.columns = columns
        
        # Aplica a limpeza em todas as células
        for col in df.columns:
            df[col] = df[col].apply(clean_value)
        
        # Converte colunas numéricas para float
        numeric_cols = columns[2:]  # Todas exceto data e horas
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Converte a coluna de data
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
        
        # Converte a coluna de horas para tempo
        df['horas'] = pd.to_datetime(df['horas'], format='%H:%M', errors='coerce').dt.time
        
        return df
    
    except Exception as e:
        logging.error(f"Erro ao processar {file_path.name}: {str(e)}")
        return pd.DataFrame()

def main():
    db = NeonDB()
    db.create_table()
    
    processed_files = []
    for file in RAW_DATA_DIR.iterdir():
        if file.is_file() and file.suffix == '.csv':
            logging.info(f"Processando: {file.name}")
            df = process_file(file)
            
            if not df.empty:
                # Salvar versão processada
                processed_path = PROCESSED_DIR / f"processed_{file.stem}.csv"
                df.to_csv(processed_path, index=False)
                
                # Enviar para o Neon
                db.upload_data(df)
                processed_files.append(file.name)
    
    logging.info(f"Processamento completo! {len(processed_files)} arquivos enviados")

if __name__ == "__main__":
    main()