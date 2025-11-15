import os
import time
import logging

import pandas as pd
import yaml
from sqlalchemy import create_engine, text

# Config yükleme fonksiyonu
def load_config():
    """
    config.yaml dosyasını okuyup bir Python dict olarak döner.
    """
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config

#  Logging ayarları
def setup_logging(log_file_path: str):
    """
    Hem dosyaya hem de konsola log yazacak basit bir logging ayarı.
    """
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logging.getLogger().handlers.clear()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file_path, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

# Veritabanı bağlantı fonksiyonu

def get_engine(db_conf: dict):
    """
    Config'teki db bilgilerini kullanarak SQLAlchemy engine döner.
    """
    user = db_conf["user"]
    password = db_conf["password"]
    host = db_conf["host"]
    port = db_conf["port"]
    name = db_conf["name"]

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(conn_str)

# Ana ETL fonksiyonu
def run_etl(engine, data_file_path: str):
    logging.info("ETL started.")

    if not os.path.exists(data_file_path):
        logging.error(f"Data file not found: {data_file_path}")
        print(f"[ERROR] Data file not found: {data_file_path}")
        return None

    logging.info("Reading CSV file...")
    df = pd.read_csv(data_file_path, encoding="latin1")

    logging.info("Transforming data...")

    # tarih
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    # toplam fiyat
    df["total_price"] = df["Quantity"] * df["UnitPrice"]

    # iptal edilmiş fatura mı
    df["is_cancelled"] = df["InvoiceNo"].astype(str).str.startswith("C")

    # customer valid mi
    df["customer_valid"] = df["CustomerID"].notna()

    # description boş olanları atar
    df = df.dropna(subset=["Description"])

    # quantity > 0 olanlar clean'e gidecek
    df_clean = df[df["Quantity"] > 0].copy()

    # kolon isimlerini DB'ye göre değiştirir
    rename_map = {
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country",
    }

    df_stg = df.rename(columns=rename_map)
    df_clean_final = df_clean.rename(columns=rename_map)

    # 4.4) LOAD - STAGING
    logging.info("Loading data into stg_retail...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stg_retail;"))

    df_stg[[
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
        "total_price",
        "is_cancelled",
        "customer_valid"
    ]].to_sql("stg_retail", engine, if_exists="append", index=False)

    # 4.5) LOAD - CLEAN
    logging.info("Loading data into retail_clean...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE retail_clean;"))

    df_clean_final[[
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
        "total_price",
        "is_cancelled",
        "customer_valid"
    ]].to_sql("retail_clean", engine, if_exists="append", index=False)

    logging.info("ETL finished successfully.")

    summary = {
        "total_rows": len(df),
        "clean_rows": len(df_clean_final),
        "cancelled_ratio": df["is_cancelled"].mean(),
        "valid_customer_ratio": df["customer_valid"].mean(),
    }
    return summary

# main bloğu
def main():
    start_time = time.time()

    config = load_config()

    setup_logging(config["paths"]["log_file"])

    logging.info("=== Online Retail ETL Pipeline started ===")

    try:
        engine = get_engine(config["db"])

        summary = run_etl(engine, config["paths"]["data_file"])

        if summary is None:
            logging.error("ETL failed due to missing data file.")
            return

        duration = time.time() - start_time

        print("\nETL Report:")
        print("-----------")
        print(f"Total Rows Loaded: {summary['total_rows']}")
        print(f"Clean Rows:       {summary['clean_rows']}")
        print(f"Cancelled Ratio:  {summary['cancelled_ratio']:.2%}")
        print(f"Valid Customers:  {summary['valid_customer_ratio']:.2%}")
        print(f"ETL Duration:     {duration:.2f} seconds")
        print("Status:           SUCCESS")

        logging.info(f"ETL completed in {duration:.2f} seconds.")
    except Exception as e:
        logging.exception("ETL failed with an unexpected error.")
        print("\n[ERROR] ETL failed. See logs for details.")
        print(str(e))

if __name__ == "__main__":
    main()
