import os
import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

class NeonDB:
    def __init__(self):
        self.conn_str = os.getenv("NEON_DATABASE_URL")
        self.engine = create_engine(self.conn_str)
    
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
        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
    
    def upload_data(self, df: pd.DataFrame):
        """Envia dados para o Neon"""
        # Verifica se o DataFrame tem dados
        if df.empty:
            return
        
        # Prepara os dados para inserção
        records = []
        for _, row in df.iterrows():
            record = (
                row['data'], row['horas'], row['precipitacao_total'], 
                row['pressao_atm_estacao'], row['pressao_atm_max'], 
                row['pressao_atm_min'], row['radiacao_global'], 
                row['temperatura_ar'], row['temperatura_orvalho'], 
                row['temperatura_max'], row['temperatura_min'], 
                row['temperatura_orvalho_max'], row['temperatura_orvalho_min'], 
                row['umidade_rel_max'], row['umidade_rel_min'], 
                row['umidade_relativa'], row['vento_direcao'], 
                row['vento_rajada_max'], row['vento_velocidade']
            )
            records.append(record)
        
        # Query de inserção
        insert_sql = """
        INSERT INTO meteo_data (
            data, hora, precipitacao_total, pressao_atm_estacao, 
            pressao_atm_max, pressao_atm_min, radiacao_global, 
            temperatura_ar, temperatura_orvalho, temperatura_max, 
            temperatura_min, temperatura_orvalho_max, temperatura_orvalho_min,
            umidade_rel_max, umidade_rel_min, umidade_relativa,
            vento_direcao, vento_rajada_max, vento_velocidade
        ) VALUES %s
        ON CONFLICT (data, hora) DO UPDATE SET
            precipitacao_total = EXCLUDED.precipitacao_total,
            pressao_atm_estacao = EXCLUDED.pressao_atm_estacao,
            pressao_atm_max = EXCLUDED.pressao_atm_max,
            pressao_atm_min = EXCLUDED.pressao_atm_min,
            radiacao_global = EXCLUDED.radiacao_global,
            temperatura_ar = EXCLUDED.temperatura_ar,
            temperatura_orvalho = EXCLUDED.temperatura_orvalho,
            temperatura_max = EXCLUDED.temperatura_max,
            temperatura_min = EXCLUDED.temperatura_min,
            temperatura_orvalho_max = EXCLUDED.temperatura_orvalho_max,
            temperatura_orvalho_min = EXCLUDED.temperatura_orvalho_min,
            umidade_rel_max = EXCLUDED.umidade_rel_max,
            umidade_rel_min = EXCLUDED.umidade_rel_min,
            umidade_relativa = EXCLUDED.umidade_relativa,
            vento_direcao = EXCLUDED.vento_direcao,
            vento_rajada_max = EXCLUDED.vento_rajada_max,
            vento_velocidade = EXCLUDED.vento_velocidade
        """
        
        # Executa a inserção usando psycopg2 diretamente
        conn = psycopg2.connect(self.conn_str)
        cur = conn.cursor()
        try:
            execute_values(
                cur,
                insert_sql,
                records,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            conn.commit()
            logging.info(f"✅ {len(records)} registros inseridos/atualizados")
        except Exception as e:
            logging.error(f"❌ Erro ao inserir dados: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    def get_data(self, query: str = "SELECT * FROM meteo_data"):
        """Recupera dados do banco"""
        return pd.read_sql(query, self.engine)
        
