from sqlalchemy import create_engine # pyright: ignore[reportMissingImports]
import os
from source.config.settings import (
    POSTGRES_USER,
    POSTGRES_PASSWORD
)


def get_engine(): 
    """Returns a sqlalchemy engine that connects to the postgresql database."""

    return create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/nba_mvp')