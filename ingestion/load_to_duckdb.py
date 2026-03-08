import duckdb
import pandas as pd
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()

raw_data_path = Path(os.getenv("RAW_DATA_PATH", "data/raw"))
duckdb_path = Path(os.getenv("DUCKDB_PATH", "data/olist.duckdb"))

TABLES = {
    "raw_customers": "olist_customers_dataset.csv",
    "raw_geolocation": "olist_geolocation_dataset.csv",
    "raw_order_items": "olist_order_items_dataset.csv",
    "raw_order_payments": "olist_order_payments_dataset.csv",
    "raw_order_reviews": "olist_order_reviews_dataset.csv",
    "raw_orders": "olist_orders_dataset.csv",
    "raw_products": "olist_products_dataset.csv",
    "raw_sellers": "olist_sellers_dataset.csv",
    "raw_product_category_name_translation": "product_category_name_translation.csv",
}

def load_csv_to_duckdb(conn: duckdb.DuckDBPyConnection, table_name: str, csv_file: Path):
    logger.info(f"carregando dados {csv_file} na tabela {table_name}...")
    df = pd.read_csv(csv_file)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    logger.info(f"Ingestão {len(df)} de dados na tabela {table_name}.")
    logger.success(f"Dados carregados com sucesso na tabela {table_name} no DuckDB.")

def main():
    logger.info("Iniciando processo de ingestão para DuckDB...")
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(duckdb_path)) as conn:
        for table_name, csv_file in TABLES.items():
            file_path = raw_data_path / csv_file
            if not file_path.exists():
                logger.error(f"Arquivo {file_path} não encontrado. Pulando tabela {table_name}.")
                continue
            load_csv_to_duckdb(conn, table_name, file_path)

    logger.info("Processo de ingestão para DuckDB concluído com sucesso.")

if __name__ == "__main__":
    main()
