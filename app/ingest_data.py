import requests
import zipfile
import io
from pathlib import Path

# A URL base para os dados de voos
BASE_URL = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

# O diretório onde os dados brutos (CSV) serão salvos
# Corresponde ao mapeamento de volume no docker-compose.yml
RAW_DATA_DIR = Path("/opt/airflow/data")

def fetch_flight_data(year: int, month: int):

    # Cria o diretório de dados brutos se ele não existir
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Formata a URL com o ano e mês desejados
    url = BASE_URL.format(year=year, month=month)
    print(f"Baixando dados de {url}...")

    try:
        # Faz a requisição para baixar o arquivo
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Lança um erro se a requisição falhar
        print("Download completo. Extraindo arquivo...")
        
        # Usa ZipFile para extrair o conteúdo do .zip em memória
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for member in z.infolist():
                # Procura pelo arquivo .csv
                if member.filename.endswith('.csv'):
                    # Define o caminho de destino para o arquivo CSV
                    target_path = RAW_DATA_DIR / member.filename
                    print(f"Extraindo {member.filename} para {target_path}...")
                    
                    # Extrai o arquivo CSV para o diretório de dados
                    with open(target_path, "wb") as f:
                        f.write(z.read(member.filename))
                    
                    print(f"Arquivo {member.filename} salvo com sucesso!")
                    return

        print("Nenhum arquivo .csv encontrado no .zip.")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar os dados: {e}")
    except zipfile.BadZipFile:
        print("Erro: O arquivo baixado não é um .zip válido.")

if __name__ == "__main__":
    # Exemplo de execução: baixar dados de Janeiro de 2024
    fetch_flight_data(year=2024, month=1)