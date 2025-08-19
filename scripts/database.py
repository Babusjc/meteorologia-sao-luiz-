import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging

load_dotenv()

class NeonDB:
    def __init__(self):
        self.conn = None
        try:
            self.conn = self.connect()
            logging.info("Conexão com o banco de dados estabelecida com sucesso")
        except Exception as e:
            logging.error(f"Falha ao conectar ao banco de dados: {str(e)}")
    
    def connect(self):
        """Estabelece conexão com o banco de dados Neon"""
        return psycopg2.connect(os.getenv("NEON_DB_URL"))
    
    def get_data(self, query):
        """Executa uma query e retorna um DataFrame"""
        if not self.conn:
            logging.error("Sem conexão com o banco de dados")
            return pd.DataFrame()
            
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            logging.error(f"Database error: {e}")
            return pd.DataFrame()
    
    def insert_data(self, df, table_name):
        """Insere dados de um DataFrame em uma tabela"""
        if not self.conn:
            logging.error("Sem conexão com o banco de dados")
            return False
            
        if df.empty:
            return False
        
        try:
            with self.conn.cursor() as cur:
                # Criar string de colunas
                cols = ",".join([f'"{col}"' for col in df.columns])
                
                # Criar string de placeholders
                placeholders = ",".join(["%s"] * len(df.columns))
                
                # Criar query
                query = f"""
                    INSERT INTO {table_name} ({cols})
                    VALUES ({placeholders})
                    ON CONFLICT (datetime) DO NOTHING
                """
                
                # Converter DataFrame para lista de tuplas
                data = [tuple(row) for row in df.to_numpy()]
                
                # Executar inserções
                cur.executemany(query, data)
                self.conn.commit()
                return True
        except Exception as e:
            logging.error(f"Insert error: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def close(self):
        """Fecha a conexão com o banco de dados"""
        if self.conn:
            self.conn.close()
            self.conn = None
