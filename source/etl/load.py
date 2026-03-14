import psycopg2 # pyright: ignore[reportMissingModuleSource]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from sqlalchemy import text # pyright: ignore[reportMissingImports]
from source.db.connection import get_engine
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

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
        logger.info('Loading data in %s.%s with %d rows', schema, table_name, len(df))

        with engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE {schema}.{table_name};'))
    else:
        logger.info('Appending %d rows to %s.%s', len(df), schema, table_name)

    df.to_sql(table_name,
            engine, 
            schema=schema,
            if_exists='append',
            index=False)

    
def load_leaderboard_local():
    """
    Loads a leaderboard view into a local CSV file.
    """

    engine = get_engine(user='app')
    query = """
        SELECT 
            RANK() OVER(
                ORDER BY "Predicted_Share" DESC
            ) AS "Rank",
            * 
        FROM serving.leaderboard
    """

    df = pd.read_sql(query, engine)

    path = Path('/opt/airflow/streamlit/data/leaderboard.csv')

    logger.info('Loading data to local CSV at %s with %d rows', path, len(df))

    df.to_csv(path, index=False)