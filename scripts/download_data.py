import gdown
import os
import requests
import argparse
from urllib.parse import urlparse, parse_qs

# Mapeamento direto dos IDs dos arquivos
FILE_MAPPING = {
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2007.csv": "1kOwFv8wWOy6xGfdLZZNKsLG9C5mRceFP",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2008.csv": "1DnYyfo25569GKsc8U4fEoLsDskSueyM7",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2009.csv": "14jjwEa_hGjjqbSgJMXOpx9Kn2Bv-2aT_",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2010.csv": "1hN9Kws7KkzTqs7R1qOURcPPvbgNz38Xu",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2011.csv": "1ytyQBlOtj52Xn6r6G9xYH0r3RCBkPCdD",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2012.csv": "1q6xehG-MV8B53PyIyUh2ujZN_Dowyk_1",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2013.csv": "1597PJ2so-UasuFf3iT8_eOiYtn70kQM9",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2014.csv": "1Ol32YOxpl0IpPYTrTQf1oRw_Vhew92ao",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2015.csv": "1wESU_esmvbGjpuA-cqIzOTOTjuzxmaNP",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2016.csv": "1zQQ0HXdTEt_IBMXKvwxzGUZMCVVkeAL2",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2017.csv": "1af5kCVedskIvJNWGN5nPOSph4GWwBzm-",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2018.csv": "1D7W1K3l8sZ9ie-OaNs-zgFxwZPKE89bV",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2019.csv": "1eDiAfhqFJNtezUiNpTh56DhDp1O634m0",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2020.csv": "1OivsiJZYvjgPjknExt9Pp9nc1ciHr_h9",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2021.csv": "1LRRN5Q-gp326g2vNEJw6-ZBVEbMXlPlP",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2022.csv": "16lKhoCA-RPgMJRG-yfX5OkeyuX70IMN3",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2023.csv": "1Tc5LsKaDPxJ2qKnrvkbAoc1S51gT0h6F",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2024.csv": "1bNZ0y3oBNY_-TIgPBQA_7kvw6Deftm1Z",
    "INMET_SE_SP_A740_SAO_LUIZ_DO_PARAITINGA_2025.csv": "11VB3rdtq35SDW0tng8E_sgyXhCcfR4MM"
}

def download_from_drive(force=False):
    os.makedirs("data/raw", exist_ok=True)
    
    for filename, file_id in FILE_MAPPING.items():
        output_path = os.path.join("data/raw", filename)
        
        # Se o arquivo já existe e não foi forçado, pular
        if os.path.exists(output_path) and not force:
            print(f"Skipping existing file: {filename}")
            continue
            
        # URL de download com confirmação
        url = f"https://drive.google.com/uc?id={file_id}&export=download&confirm=t"
        
        try:
            # Usar requests para baixar o arquivo
            session = requests.Session()
            response = session.get(url, stream=True)
            
            # Salvar o arquivo
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=32768):
                    if chunk:
                        f.write(chunk)
            
            print(f"Downloaded: {filename}")
            
        except Exception as e:
            print(f"Failed to download {filename}: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download de dados meteorológicos do Google Drive')
    parser.add_argument('--force', action='store_true', 
                       help='Forçar download mesmo se arquivo já existir localmente')
    args = parser.parse_args()
    
    download_from_drive(force=args.force)
