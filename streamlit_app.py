import sys
from pathlib import Path

# --- INÍCIO DO BLOCO DE AJUSTE DE PATH PARA O STREAMLIT --- #
# Este script (streamlit_app.py) está na raiz do repositório.
# Ele precisa adicionar a pasta ".app" ao sys.path para que o Streamlit
# possa encontrar e executar o dashboard.py que está lá dentro.
app_dir = Path(__file__).parent / ".app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
# --- FIM DO BLOCO DE AJUSTE DE PATH PARA O STREAMLIT --- #

# Agora, importa e executa o seu dashboard real
# Como .app foi adicionado ao sys.path, podemos importar 'dashboard' diretamente
# e o Python encontrará o arquivo 'dashboard.py' dentro de '.app/'.
import dashboard

# Como o seu dashboard.py já é um script completo do Streamlit,
# apenas a importação já é suficiente para que o Streamlit
# comece a processar o conteúdo do arquivo.
