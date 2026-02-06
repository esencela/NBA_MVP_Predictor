import psycopg2 # pyright: ignore[reportMissingModuleSource]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from sqlalchemy import text # pyright: ignore[reportMissingImports]
from source.db.connection import get_engine


def load_to_database(df: pd.DataFrame, user: str, table_name: str, schema: str, append: bool = False):
    """
    Loads a given DataFrame into PostgreSQL database.
    
    Params:
        df (pd.DataFrame): DataFrame to be loaded into database.
        user (str): Database user to connect as. Must be one of 'etl', 'ml', or 'app'.
        table_name (str): Name of target table in database.
        schema (str): Name of schema of target table in database.
        append (bool): If True, appends data to existing table; if False, replaces the table.
    """
    engine = get_engine(user)

    if not append:
        # Truncate table data to avoid issues with dependent views
        with engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE {schema}.{table_name};'))
        # Ensure cascade for tables that have dependencies
        #with engine.begin() as conn:
        #    conn.execute(f'DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;')

    df.to_sql(table_name,
            engine, 
            schema=schema,
            if_exists='append',
            index=False)


def check_schema(engine, schema: str):
    """
    Creates a schema in target database if it does not exist.

    Params:
        engine (sqlalchemy.engine.Engine): The engine connecting to the PostgreSQL database.
        schema (str): Name of schema in database.
    """

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))