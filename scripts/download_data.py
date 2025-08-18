import gdown
import os
from pathlib import Path

def download_from_drive():
    # URL do diretório do Google Drive
    folder_url = "https://drive.google.com/drive/folders/1E1MRLUhKHSv6AN1ECagQi2O6NRG1SEUn"
    
    # Diretório local para salvar os arquivos
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Baixa todos os arquivos do diretório
    gdown.download_folder(
        folder_url,
        output=str(output_dir),
        quiet=False,
        use_cookies=False
    )
    
    # Lista arquivos baixados
    downloaded_files = list(output_dir.glob('*'))
    print(f"Baixados {len(downloaded_files)} arquivos do Google Drive")

if __name__ == "__main__":
    download_from_drive()
