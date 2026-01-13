from sqlalchemy import create_engine
import os
from src.config.settings import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)

def get_engine(): 
    """Returns a sqlalchemy engine that connects to the postgresql database"""

    return create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')