import requests
import json
import time
from pathlib import Path
import pandas as pd

# Pasta principal
base = Path("dataset")

# Subpastas
for nome in ["raw", "bronze", "silver", "gold"]:
    (base / nome).mkdir(parents=True, exist_ok=True)

# Configuração
BASE_URL = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data/"
RAW_DIR = Path("dataset/raw")
BRONZE_DIR = Path("dataset/bronze")
API_TOKEN = "0a3c26398bb82d107772cf4a05967ebbb7c95bcc"
MAX_PAGES = 1000

RAW_DIR.mkdir(exist_ok=True)
BRONZE_DIR.mkdir(exist_ok=True)


def get_existing_pages():
    return {int(f.stem.replace("page_", "")) for f in RAW_DIR.glob("page_*.json")}


def fetch_data():
    headers = {"Authorization": f"Token {API_TOKEN}"}
    existing_pages = get_existing_pages()
    page = 1

    while page <= MAX_PAGES:
        if page in existing_pages:
            print(f"Página {page} já existe, pulando")
            page += 1
            continue

        try:
            response = requests.get(
                BASE_URL, headers=headers, params={"page": page})

            if response.status_code == 429:
                print(f"Rate limit atingido, aguardando 60s")
                time.sleep(60)
                continue

            response.raise_for_status()
            data = response.json()

            with open(RAW_DIR / f"page_{page}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"Página {page} salva")

            page += 1
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Erro na página {page}: {e}")
            time.sleep(30)


def process_to_parquet():
    all_data = []

    for file in sorted(RAW_DIR.glob("page_*.json")):
        with open(file, encoding="utf-8") as f:
            data = json.load(f)
            all_data.extend(data.get("results", []))

    if not all_data:
        print("Nenhum dado encontrado para processar")
        return

    df = pd.DataFrame(all_data)
    print(f"Total de registros: {len(df)}")

    for (ano, mes), group in df.groupby(["ano", "mes"]):
        output_dir = BRONZE_DIR / f"ano={ano}" / f"mes={mes}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "data.parquet"
        group.to_parquet(output_file, index=False, engine="pyarrow")
    print(f"Partição para ano e mes criadas!")


if __name__ == "__main__":
    print("Iniciando coleta de dados")
    fetch_data()
    print("\nProcessando para Parquet")
    process_to_parquet()
    print("Concluído")

