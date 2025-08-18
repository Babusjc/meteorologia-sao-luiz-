import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import re

# Adiciona o diretório pai ao path para resolver imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Agora importe o módulo database
from scripts.database import NeonDB
from dotenv import load_dotenv

# Configuração
load_dotenv()
RAW_DATA_DIR = Path("data/raw")  # Agora usando caminho relativo
PROCESSED_DIR = Path("data/processed")
os.makedirs(RAW_DATA_DIR, exist_ok=True)
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
        
        # Verifica se o número de colunas corresponde
        if len(df.columns) != len(columns):
            logging.warning(f"O arquivo {file_path.name} tem {len(df.columns)} colunas, mas esperávamos {len(columns)}. Verificando correspondência...")
            
            # Tenta usar as colunas existentes se o número for diferente
            df.columns = df.columns[:len(columns)]
        else:
            df.columns = columns
        
        # Aplica a limpeza em todas as células
        for col in df.columns:
            df[col] = df[col].apply(clean_value)
        
        # Converte colunas numéricas para float
        numeric_cols = columns[2:]  # Todas exceto data e horas
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Converte a coluna de data
        if 'data' in df.columns:
            df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
        else:
            logging.error("Coluna 'data' não encontrada no arquivo")
        
        # Converte a coluna de horas para tempo
        if 'horas' in df.columns:
            try:
                # Tenta converter para datetime e extrair o tempo
                df['horas'] = pd.to_datetime(df['horas'], format='%H:%M', errors='coerce').dt.time
            except Exception as e:
                logging.error(f"Erro ao converter horas: {str(e)}")
                # Tenta uma conversão alternativa
                df['horas'] = pd.to_datetime(df['horas'], errors='coerce').dt.time
        else:
            logging.error("Coluna 'horas' não encontrada no arquivo")
        
        return df
    
    except Exception as e:
        logging.error(f"Erro ao processar {file_path.name}: {str(e)}")
        return pd.DataFrame()

def main():
    try:
        db = NeonDB()
        db.create_table()
    except Exception as e:
        logging.error(f"Falha ao conectar ao banco de dados: {str(e)}")
        return
    
    processed_files = []
    
    # Lista todos os arquivos no diretório
    files = list(RAW_DATA_DIR.glob('*.csv'))
    if not files:
        logging.warning(f"Nenhum arquivo CSV encontrado em {RAW_DATA_DIR}")
        return
    
    for file in files:
        if file.is_file() and file.suffix.lower() == '.csv':
            logging.info(f"Processando: {file.name}")
            try:
                df = process_file(file)
                
                if not df.empty:
                    # Salvar versão processada
                    processed_path = PROCESSED_DIR / f"processed_{file.stem}.csv"
                    os.makedirs(processed_path.parent, exist_ok=True)
                    df.to_csv(processed_path, index=False)
                    
                    # Enviar para o Neon
                    db.upload_data(df)
                    processed_files.append(file.name)
            except Exception as e:
                logging.error(f"Erro ao processar {file.name}: {str(e)}")
    
    logging.info(f"Processamento completo! {len(processed_files)} arquivos enviados")

if __name__ == "__main__":
    main()
