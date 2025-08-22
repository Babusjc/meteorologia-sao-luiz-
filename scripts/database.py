# scripts/database.py

from __future__ import annotations
import os
import logging
from typing import Optional, Iterable, List

import pandas as pd
import streamlit as st
import psycopg2
import psycopg2.extras as pg_extras


# -----------------------------
# Uso no Streamlit (dashboard)
# -----------------------------

def get_data_from_db(query: str) -> pd.DataFrame:
    """
    Executa uma consulta no banco de dados configurado nos Secrets do Streamlit
    e retorna os resultados como um DataFrame do Pandas.

    Requer em .streamlit/secrets.toml:
    [connections.neon_db]
    url = "postgresql://<user>:<pass>@<host>/<db>?sslmode=require"
    """
    try:
        conn = st.connection("neon_db", type="sql")
        logging.info("Conexão com o banco de dados estabelecida via st.connection.")
        df = conn.query(query)
        logging.info(f"Consulta executada com sucesso, {len(df)} linhas retornadas.")
        return df
    except Exception as e:
        logging.error(f"Falha ao conectar ou executar a consulta: {e}")
        st.error(f"Erro ao acessar o banco de dados: {e}")
        return pd.DataFrame()


# ----------------------------------------
# Uso em scripts (ETL/CI) fora do Streamlit
# ----------------------------------------

class ETLDB:
    """
    Conexão direta via psycopg2 para processos de ETL e CI (GitHub Actions).
    Usa a variável de ambiente NEON_DB_URL.
    """

    def __init__(self) -> None:
        self.conn_string: Optional[str] = os.getenv("NEON_DB_URL")
        if not self.conn_string:
            raise ValueError("NEON_DB_URL environment variable not set.")
        try:
            self.conn = psycopg2.connect(self.conn_string)
            self.cursor = self.conn.cursor()
            logging.info("Conexão com o banco de dados para ETL estabelecida.")
            self._ensure_table()
        except Exception as e:
            logging.error(f"Erro ao conectar ao banco de dados para ETL: {e}")
            raise

    def _ensure_table(self) -> None:
        """Cria a tabela meteo_data se não existir, com índice único (data, hora)."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS meteo_data (
            data DATE NOT NULL,
            hora TIME NOT NULL,
            precipitacao_total DOUBLE PRECISION,
            pressao_atm_estacao DOUBLE PRECISION,
            temperatura_ar DOUBLE PRECISION,
            umidade_relativa DOUBLE PRECISION,
            vento_velocidade DOUBLE PRECISION,
            vento_direcao DOUBLE PRECISION,
            radiacao_global DOUBLE PRECISION,
            temperatura_max DOUBLE PRECISION,
            temperatura_min DOUBLE PRECISION,
            PRIMARY KEY (data, hora)
        );
        """
        self.cursor.execute(create_sql)
        self.conn.commit()

    def insert_data(self, df: pd.DataFrame, table: str = "meteo_data") -> bool:
        """
        Insere (upsert) dados em lote no banco.
        Espera colunas compatíveis com a tabela meteo_data.
        """
        if df is None or df.empty:
            logging.warning("DataFrame vazio — nada a inserir.")
            return True

        expected_cols: List[str] = [
            "data", "hora",
            "precipitacao_total", "pressao_atm_estacao", "temperatura_ar",
            "umidade_relativa", "vento_velocidade", "vento_direcao",
            "radiacao_global", "temperatura_max", "temperatura_min",
        ]

        # Reindexa e converte NaN -> None para permitir INSERT
        df_to_insert = (
            df.reindex(columns=expected_cols)
              .copy()
        )

        # Converte tipos de data/hora se vierem como string/objeto
        if "data" in df_to_insert.columns:
            df_to_insert["data"] = pd.to_datetime(df_to_insert["data"], errors="coerce").dt.date
        if "hora" in df_to_insert.columns:
            # aceita strings HH:MM ou datetime.time
            df_to_insert["hora"] = pd.to_datetime(df_to_insert["hora"], errors="coerce").dt.time

        df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)

        cols_sql = ", ".join(expected_cols)
        placeholders = ", ".join(["%s"] * len(expected_cols))
        update_assignments = ", ".join([f"{c} = EXCLUDED.{c}" for c in expected_cols[2:]])  # pula PK

        upsert_sql = f"""
            INSERT INTO {table} ({cols_sql})
            VALUES ({placeholders})
            ON CONFLICT (data, hora)
            DO UPDATE SET {update_assignments};
        """

        values = [tuple(row[c] for c in expected_cols) for _, row in df_to_insert.iterrows()]

        try:
            pg_extras.execute_values(self.cursor, upsert_sql.replace("VALUES (%s)", "VALUES %s"), values)
            self.conn.commit()
            logging.info(f"Upsert concluído com {len(values)} linhas em {table}.")
            return True
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Erro ao inserir dados (upsert) em {table}: {e}")
            return False

    def execute_query(self, query: str, params: Optional[Iterable] = None) -> None:
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            logging.info("Consulta ETL executada com sucesso.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Erro ao executar consulta ETL: {e}")
            raise

    def fetch_data(self, query: str, params: Optional[Iterable] = None) -> pd.DataFrame:
        try:
            self.cursor.execute(query, params)
            columns = [desc[0] for desc in self.cursor.description]
            data = self.cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            logging.error(f"Erro ao buscar dados ETL: {e}")
            raise

    def close(self) -> None:
        try:
            if getattr(self, "cursor", None):
                self.cursor.close()
            if getattr(self, "conn", None):
                self.conn.close()
            logging.info("Conexão com o banco de dados para ETL fechada.")
        except Exception:
            pass



