from source.db.connection import get_engine
import pathlib

def remove_season_data(season: int):
    """
    Removes data for the specified season from the given table in the database.
    
    Params:
        season (int): The season year to be removed from the table.
    """

    engine = get_engine(user='etl')

    with engine.begin() as conn:
        query = f"""
        DELETE FROM stats.player_features
        WHERE "Season" = {season};

        DELETE FROM stats.player_stats
        WHERE "Season" = {season};
        """

        conn.execute(query)


def create_serving_view():
    """Creates a view in PostgreSQL database that queries player stats for players with predicted vote share above 0."""

    SQL_FILE_PATH = "/opt/airflow/sql/serving.sql"

    sql_path = pathlib.Path(SQL_FILE_PATH)

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_FILE_PATH}")
    
    sql = sql_path.read_text()

    engine = get_engine(user='ml')

    with engine.begin() as conn:
        conn.execute(sql)