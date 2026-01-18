import psycopg2 # pyright: ignore[reportMissingModuleSource]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from sqlalchemy import text
from source.db.connection import get_engine


def load_to_database(df: pd.DataFrame, table_name: str, schema: str):
    """
    Loads a given DataFrame into PostgreSQL database.
    
    Params:
        df (pd.DataFrame): DataFrame to be loaded into database.
        table_name (str): Name of target table in database.
        schema (str): Name of schema of target table in database.
    """
    engine = get_engine()

    check_schema(engine, schema)

    df.to_sql(table_name,
            engine, 
            schema=schema,
            if_exists='replace', 
            index=False)


def check_schema(engine, schema: set):

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))