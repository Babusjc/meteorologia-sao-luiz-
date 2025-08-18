import gdown
import os
import re
import requests
from bs4 import BeautifulSoup

def download_from_drive():
    # Criar diretório de destino
    os.makedirs("data/raw", exist_ok=True)
    
    # URL do Google Drive
    folder_url = "https://drive.google.com/drive/folders/1kOwFv8wWOy6xGfdLZZNKsLG9C5mRceFP"
    
    # Obter lista de arquivos usando scraping
    session = requests.Session()
    response = session.get(folder_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Encontrar todos os links de arquivo
    file_links = []
    for a in soup.find_all('a', {'data-tooltip-unhoverable': 'true'}):
        href = a.get('href')
        if href and 'file/d/' in href:
            file_id = href.split('/')[5]
            file_name = a.text.strip()
            if file_name:
                file_links.append((file_id, file_name))
    
    # Baixar cada arquivo
    for file_id, original_name in file_links:
        # Sanitizar nome do arquivo
        safe_name = re.sub(r'[^a-zA-Z0-9._-]', '_', original_name)
        output_path = os.path.join("data/raw", safe_name)
        
        # URL de download direto
        download_url = f"https://drive.google.com/uc?id={file_id}&export=download"
        
        # Baixar arquivo
        gdown.download(
            download_url, 
            output_path, 
            quiet=False
        )
        print(f"Downloaded: {safe_name}")

if __name__ == "__main__":
    download_from_drive()
