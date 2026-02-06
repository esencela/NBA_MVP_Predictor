from sqlalchemy import create_engine # pyright: ignore[reportMissingImports]
import os
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.config.settings import (
    ETL_POSTGRES_USER,
    ETL_POSTGRES_PASSWORD,
    ML_POSTGRES_USER,
    ML_POSTGRES_PASSWORD,
    APP_POSTGRES_USER,
    APP_POSTGRES_PASSWORD
)

DB_CONFIG = {
    'etl': {
        'username': ETL_POSTGRES_USER,
        'password': ETL_POSTGRES_PASSWORD
    },
    'ml': {
        'username': ML_POSTGRES_USER,
        'password': ML_POSTGRES_PASSWORD
    },
    'app': {
        'username': APP_POSTGRES_USER,
        'password': APP_POSTGRES_PASSWORD
    }
}


def get_engine(user: str): 
    """Returns a SQLalchemy engine that connects to the PostgreSQL database."""

    if user not in DB_CONFIG:
        raise ValueError(f"Invalid user '{user}'. Valid options are: {list(DB_CONFIG.keys())}")
    
    config = DB_CONFIG[user]

    return create_engine(f'postgresql+psycopg2://{config["username"]}:{config["password"]}@postgres:5432/nba_mvp')


def query_data(query: str, user: str) -> pd.DataFrame:
    """Queries PostgreSQL and returns a dataframe of results."""

    engine = get_engine(user)

    df = pd.read_sql(query, engine)

    return df