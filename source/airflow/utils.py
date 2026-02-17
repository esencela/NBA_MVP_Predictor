from source.db.connection import get_engine
from sqlalchemy import text # pyright: ignore[reportMissingImports]
from source.config.settings import CURRENT_SEASON


def log_data_freshness(data_freshness, **kwargs):
    
    dag_run = kwargs['dag_run']

    trigger_type = 'manual' if dag_run.external_trigger else 'scheduled'

    engine = get_engine(user='etl')

    with engine.begin() as conn:
        conn.execute(text("""
           INSERT INTO metadata.data_freshness (
               dag_id,
               run_id,
               season,
               time_updated,
               data_freshness,
               trigger_type
           )
           VALUES (
               :dag_id,
               :run_id,
               :season,
               NOW(),
               :data_freshness,
               :trigger_type
           )
        """), {
            'dag_id': kwargs['dag'].dag_id,
            'run_id': kwargs['run_id'],
            'season': CURRENT_SEASON,
            'data_freshness': data_freshness,
            'trigger_type': trigger_type
        })