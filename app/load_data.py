# app/load_data.py
import duckdb
from pathlib import Path

# Define os caminhos
DATA_DIR = Path("/opt/airflow/data")
DB_FILE = DATA_DIR / "flights.db"

# Busca pelo arquivo CSV na pasta de dados
csv_files = list(DATA_DIR.glob("*.csv"))

if not csv_files:
    print("Nenhum arquivo CSV encontrado na pasta /usr/src/data.")
else:
    csv_file_path = csv_files[0]
    print(f"Arquivo CSV encontrado: {csv_file_path.name}")

    # Conecta ao arquivo do banco de dados DuckDB (ele será criado se não existir)
    con = duckdb.connect(database=str(DB_FILE), read_only=False)

    print("Carregando dados do CSV para a tabela 'flights_raw'...")

    # Usa a função de leitura de CSV do DuckDB para criar a tabela e carregar os dados.
    # Esta é a forma mais eficiente de fazer isso.
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE flights_raw AS
            SELECT *
            FROM read_csv_auto('{csv_file_path}');
        """)

        # Verifica o resultado
        row_count = con.execute("SELECT COUNT(*) FROM flights_raw").fetchone()[0]
        print(f"Sucesso! Tabela 'flights_raw' criada com {row_count} linhas.")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        con.close()