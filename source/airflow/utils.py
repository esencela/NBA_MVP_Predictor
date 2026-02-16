from source.db.connection import get_engine
from sqlalchemy import text # pyright: ignore[reportMissingImports]
from source.config.settings import CURRENT_SEASON

def start_update_run(**kwargs):
        """
        Logs the start of the update process in the database and return the update_id for tracking.
        """
        dag_run = kwargs['dag_run']

        trigger_type = 'manual' if dag_run.external_trigger else 'scheduled'

        engine = get_engine(user='ml')

        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO metadata.update_runs (
                    dag_id,
                    run_id,
                    start_time,
                    status,
                    trigger_type
                )
                VALUES (
                    :dag_id,
                    :run_id,
                    NOW(),
                    'running',
                    :trigger_type
                )
                RETURNING update_id;
            """), 
            {
                'dag_id': kwargs['dag'].dag_id,
                'run_id': kwargs['run_id'],
                'trigger_type': trigger_type
            })

            update_id = result.scalar()
        
        return update_id
    

def log_update_success(context):
    """
    Callback function to be executed upon successful completion of DAG run.
    Updates the status of the current update run in the database to 'success' and records the end time.
    """
    
    update_id = context['ti'].xcom_pull(task_ids='start_log', key='return_value')

    engine = get_engine(user='ml')
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE metadata.update_runs
            SET status = 'success', 
                end_time = NOW()
            WHERE update_id = :update_id;
            """),
            {
                'update_id': update_id
            }
        )


def log_update_failure(context):
    """
    Callback function to be executed upon failure of DAG run.
    Updates the status of the current update run in the database to 'failed' and records the error message.
    """

    update_id = context['ti'].xcom_pull(task_ids='start_log', key='return_value')
    error_message = str(context.get('exception'))[:500] # Truncate error message to limit data usage

    engine = get_engine(user='ml')
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE metadata.update_runs
            SET status = 'failed', 
                end_time = NOW(),
                error_message = :error_message
            WHERE update_id = :update_id;
            """),
            {
                'update_id': update_id,
                'error_message': error_message
            }
        )


def log_data_freshness(data_freshness, **kwargs):
    
    dag_run = kwargs['dag_run']

    trigger_type = 'manual' if dag_run.external_trigger else 'scheduled'

    engine = get_engine(user='etl')

    with engine.begin() as conn:
        result = conn.execute(text("""
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