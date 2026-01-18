import psycopg2 # pyright: ignore[reportMissingModuleSource]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
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

    df.to_sql(table_name,
            engine, 
            schema=schema,
            if_exists='replace', 
            index=False)
