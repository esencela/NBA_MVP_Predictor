from source.db.connection import get_engine

def remove_season_data(season: int):
    """
    Removes data for the specified season from the given table in the database.
    
    Params:
        season (int): The season year to be removed from the table.
    """

    engine = get_engine()

    with engine.begin() as conn:
        query = f"""
        DELETE FROM stats.player_features
        WHERE "Season" = {season};

        DELETE FROM stats.player_stats
        WHERE "Season" = {season};
        """

        conn.execute(query)