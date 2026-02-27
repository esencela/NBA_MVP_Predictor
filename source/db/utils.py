from source.db.connection import get_engine
import logging

logger = logging.getLogger(__name__)

def remove_season_data(season: int):
    """
    Removes data for the specified season from the given table in the database.
    
    Params:
        season (int): The season year to be removed from the table.
    """

    logger.info('Removing existing data for %s season from database', season)

    engine = get_engine(user='etl')

    with engine.begin() as conn:
        query = f"""
        DELETE FROM stats.player_features
        WHERE "Season" = {season};

        DELETE FROM stats.player_stats
        WHERE "Season" = {season};
        """

        conn.execute(query)