import os
import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
load_dotenv()

class NeonDB:
    def __init__(self):
        self.conn_str = os.getenv("NEON_DATABASE_URL")
        if not self.conn_str:
            logging.error("NEON_DATABASE_URL não encontrada nas variáveis de ambiente")
            raise ValueError("String de conexão do Neon não configurada")
        
        try:
            self.engine = create_engine(self.conn_str)
            logging.info("Conexão com o banco de dados estabelecida com sucesso")
        except Exception as e:
            logging.error(f"Erro ao criar engine: {str(e)}")
            raise
    
    def create_table(self):
        """Cria a tabela se não existir com a estrutura definida"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS meteo_data (
            id SERIAL PRIMARY KEY,
            data DATE NOT NULL,
            hora TIME NOT NULL,
            precipitacao_total FLOAT,
            pressao_atm_estacao FLOAT,
            pressao_atm_max FLOAT,
            pressao_atm_min FLOAT,
            radiacao_global FLOAT,
            temperatura_ar FLOAT,
            temperatura_orvalho FLOAT,
            temperatura_max FLOAT,
            temperatura_min FLOAT,
            temperatura_orvalho_max FLOAT,
            temperatura_orvalho_min FLOAT,
            umidade_rel_max FLOAT,
            umidade_rel_min FLOAT,
            umidade_relativa FLOAT,
            vento_direcao FLOAT,
            vento_rajada_max FLOAT,
            vento_velocidade FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_data_hora UNIQUE (data, hora)
        );
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            logging.info("Tabela meteo_data criada/verificada com sucesso")
        except Exception as e:
            logging.error(f"Erro ao criar tabela: {str(e)}")
            raise
    
    def upload_data(self, df: pd.DataFrame):
        """Envia dados para o Neon"""
        if df.empty:
            logging.warning("DataFrame vazio, nada para enviar")
            return
        
        # Lista de colunas necessárias
        required_columns = [
            'data', 'horas', 'precipitacao_total', 'pressao_atm_estacao',
            'pressao_atm_max', 'pressao_atm_min', 'radiacao_global',
            'temperatura_ar', 'temperatura_orvalho', 'temperatura_max',
            'temperatura_min', 'temperatura_orvalho_max', 'temperatura_orvalho_min',
            'umidade_rel_max', 'umidade_rel_min', 'umidade_relativa',
            'vento_direcao', 'vento_rajada_max', 'vento_velocidade'
        ]
        
        # Verifica se todas as colunas necessárias existem
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.warning(f"Colunas ausentes no DataFrame: {', '.join(missing_columns)}")
        
        # Preparar os dados para inserção
        records = []
        for _, row in df.iterrows():
            try:
                record = tuple(
                    row[col] if col in row and not pd.isna(row[col]) else None
                    for col in required_columns
                )
                records.append(record)
            except Exception as e:
                logging.error(f"Erro ao preparar linha: {str(e)}")
                logging.error(f"Conteúdo da linha: {row}")
        
        if not records:
            logging.warning("Nenhum registro válido para enviar")
            return
        
        # Query de inserção
        columns_str = ', '.join(required_columns)
        placeholders = ', '.join(['%s'] * len(required_columns))
        update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in required_columns if col not in ['data', 'hora']])
        
        insert_sql = f"""
        INSERT INTO meteo_data ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (data, hora) DO UPDATE SET
            {update_str}
        """
        
        try:
            # Usar conexão direta do psycopg2 para execute_values
            conn = psycopg2.connect(self.conn_str)
            cur = conn.cursor()
            execute_values(
                cur,
                insert_sql,
                records,
                template=f"({', '.join(['%s']*len(required_columns))})",
                page_size=100
            )
            conn.commit()
            logging.info(f"✅ {len(records)} registros inseridos/atualizados")
        except Exception as e:
            logging.error(f"❌ Erro ao inserir dados: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def get_data(self, query: str = "SELECT * FROM meteo_data"):
        """Recupera dados do banco"""
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"Erro ao recuperar dados: {str(e)}")
            return pd.DataFrame()
