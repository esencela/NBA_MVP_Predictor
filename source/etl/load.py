import psycopg2 # pyright: ignore[reportMissingModuleSource]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.db.connection import get_engine

def load_to_database(df: pd.DataFrame, table_name: str):

    engine = get_engine()

    df.to_sql(table_name, engine, if_exists='replace', index=False)
