from dotenv import load_dotenv # pyright: ignore[reportMissingImports]
import os

load_dotenv()

CURRENT_SEASON = 2026
MIN_SEASON = 2003

LOCAL_EXTRACT = True
SLEEP_TIME = 0 #if LOCAL_EXTRACT else 3.5

ETL_POSTGRES_USER = os.getenv("ETL_POSTGRES_USER")
ETL_POSTGRES_PASSWORD = os.getenv("ETL_POSTGRES_PASSWORD")
ML_POSTGRES_USER = os.getenv("ML_POSTGRES_USER")
ML_POSTGRES_PASSWORD = os.getenv("ML_POSTGRES_PASSWORD")
APP_POSTGRES_USER = os.getenv("APP_POSTGRES_USER")
APP_POSTGRES_PASSWORD = os.getenv("APP_POSTGRES_PASSWORD")

MVP_MODEL_PATH = "/opt/airflow/models/mvp_model.pkl"
os.makedirs(os.path.dirname(MVP_MODEL_PATH), exist_ok=True)