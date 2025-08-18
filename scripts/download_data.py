import gdown
import os
import re

def download_from_drive():
    # Criar diretório de destino
    os.makedirs("data/raw", exist_ok=True)
    
    # URL do Google Drive
    url = "https://drive.google.com/drive/folders/1kOwFv8wWOy6xGfdLZZNKsLG9C5mRceFP"
    
    # Configurações para lidar com nomes de arquivos
    gdown.download_folder(
        url,
        output="data/raw",
        quiet=False,
        use_cookies=False,
        remaining_ok=True,
        fuzzy=True,
        resume_download=True
    )

    # Renomear arquivos para remover caracteres problemáticos
    for filename in os.listdir("data/raw"):
        if " " in filename or "(" in filename or ")" in filename:
            new_name = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
            os.rename(
                os.path.join("data/raw", filename),
                os.path.join("data/raw", new_name)
            )

if __name__ == "__main__":
    download_from_drive()
