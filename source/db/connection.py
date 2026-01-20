from sqlalchemy import create_engine # pyright: ignore[reportMissingImports]
import os
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.config.settings import (
    ETL_POSTGRES_USER,
    ETL_POSTGRES_PASSWORD
)


def get_engine(): 
    """Returns a SQLalchemy engine that connects to the PostgreSQL database."""

    return create_engine(f'postgresql+psycopg2://airflow:airflow@postgres:5432/nba_mvp')


def query_data(query: str) -> pd.DataFrame:
    """Queries PostgreSQL and returns a dataframe of results."""

    engine = get_engine()

    df = pd.read_sql(query, engine)

    return df