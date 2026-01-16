from sqlalchemy import create_engine # pyright: ignore[reportMissingImports]
import os
from source.config.settings import (
    ETL_POSTGRES_USER,
    ETL_POSTGRES_PASSWORD
)


def get_engine(): 
    """Returns a SQLalchemy engine that connects to the PostgreSQL database."""

    return create_engine(f'postgresql+psycopg2://{ETL_POSTGRES_USER}:{ETL_POSTGRES_PASSWORD}@localhost:5432/nba_mvp')