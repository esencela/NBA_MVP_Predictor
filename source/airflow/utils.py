from source.db.connection import get_engine
from sqlalchemy import text # pyright: ignore[reportMissingImports]

def log_update_success(context):
    """
    Callback function to be executed upon successful completion of DAG run.
    Updates the status of the current update run in the database to 'success' and records the end time.
    """
    
    update_id = context['ti'].xcom_pull(task_ids='start', key='return_value')

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

    update_id = context['ti'].xcom_pull(task_ids='start', key='return_value')
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