# /scripts/database.py

import pandas as pd
import streamlit as st
import logging

def get_data_from_db(query: str) -> pd.DataFrame:
    """
    Executa uma consulta no banco de dados configurado nos Secrets do Streamlit
    e retorna os resultados como um DataFrame do Pandas.

    Args:
        query (str): A consulta SQL a ser executada.

    Returns:
        pd.DataFrame: Um DataFrame com os resultados da consulta.
                      Retorna um DataFrame vazio em caso de erro.
    """
    try:
        # O nome "neon_db" DEVE corresponder ao que está no seu arquivo de Secrets:
        # [connections.neon_db]
        conn = st.connection("neon_db", type="sql")
        
        logging.info("Conexão com o banco de dados estabelecida via st.connection.")
        
        df = conn.query(query)
        logging.info(f"Consulta executada com sucesso, {len(df)} linhas retornadas.")
        
        return df
        
    except Exception as e:
        # Loga o erro para que ele apareça nos logs do Streamlit
        logging.error(f"Falha ao conectar ou executar a consulta: {e}")
        
        # Mostra uma mensagem de erro amigável no aplicativo
        st.error(f"Erro ao acessar o banco de dados: {e}")
        
        return pd.DataFrame()

# A classe NeonDB e as outras funções foram removidas pois não são mais necessárias
# para o dashboard. O st.connection gerencia a conexão de forma mais eficiente.

