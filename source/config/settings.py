from dotenv import load_dotenv # pyright: ignore[reportMissingImports]
import os

load_dotenv()

ETL_POSTGRES_USER = os.getenv("ETL_POSTGRES_USER")
ETL_POSTGRES_PASSWORD = os.getenv("ETL_POSTGRES_PASSWORD")
ML_POSTGRES_USER = os.getenv("ML_POSTGRES_USER")
ML_POSTGRES_PASSWORD = os.getenv("ML_POSTGRES_PASSWORD")