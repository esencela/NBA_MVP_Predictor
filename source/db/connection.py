from sqlalchemy import create_engine
import os

def get_engine():
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = 'localhost'
    port = '5432'
    db = 'nba_mvp'

    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')