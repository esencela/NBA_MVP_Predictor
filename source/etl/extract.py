import pandas as pd # pyright: ignore[reportMissingModuleSource]
from typing import List
import time
import logging
from source.config.settings import (
    CURRENT_SEASON
)

def extract_season_data(season: int) -> dict:
    """
    Extract NBA datasets for a given season from Basketball Reference, implementing sleep to avoid request limits.

    This function extracts all relevant datasets for a single NBA season:
    - NBA per-game player statistics
    - NBA advanced player statistics
    - NBA team statistics
    - NBA MVP voting data
    
    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        dict: Dictionary that holds all dataframes (per_game, advanced, team, mvp).
    """

    sleeping_time = 5

    per_game = extract_per_game_season_data(season)
    time.sleep(sleeping_time)

    advanced = extract_advanced_season_data(season)
    time.sleep(sleeping_time)

    team = extract_team_season_data(season)
    time.sleep(sleeping_time)
    
    mvp = extract_mvp_vote_data(season)
    time.sleep(sleeping_time)

    logging.info(f'Extracted data for {season} season')

    return {
        'per_game': per_game,
        'advanced': advanced,
        'team': team,
        'mvp': mvp
    }


def extract_per_game_season_data(season: int) -> pd.DataFrame:
    """
    Extract NBA per-game player statistics for a given season from Basketball Reference.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        pd.DataFrame: Raw per-game player statistics for the given season.
    """

    url = f'https://www.basketball-reference.com/leagues/NBA_{season}_per_game.html'
    tables = retrieve_tables_from_url(url)

    # Required data is kept in first table
    df = tables[0]

    # Last row contains unnecessary data, drop it from table
    df.drop(df.tail(1).index, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def extract_advanced_season_data(season: int) -> pd.DataFrame:
    """
    Extract NBA advanced player statistics for a given season from Basketball Reference.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        pd.DataFrame: Raw advanced player statistics for the given season.
    """

    url = f'https://www.basketball-reference.com/leagues/NBA_{season}_advanced.html'
    tables = retrieve_tables_from_url(url)

    # Required data is kept in first table
    df = tables[0]

    # Last row contains unnecessary data, drop it from table
    df.drop(df.tail(1).index, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    return df


def extract_team_season_data(season: int) -> dict:
    """
    Extract NBA team statistics for a given season from Basketball Reference.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        dict: Raw team statistics for the given season (east, west).
    """

    url = f'https://www.basketball-reference.com/leagues/NBA_{season}_standings.html'
    tables = retrieve_tables_from_url(url)

    # Required data is split between first two tables
    df_east = tables[0]
    df_west = tables[1]

    df_east.reset_index(drop=True, inplace=True)
    df_west.reset_index(drop=True, inplace=True)

    return {
        'east': df_east,
        'west': df_west
    }


def extract_mvp_vote_data(season: int) -> pd.DataFrame:
    """
    Extract NBA MVP voting data for a given season from Basketball Reference.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        pd.DataFrame: Raw MVP voting data for the given season.
    """

    url = f"https://www.basketball-reference.com/awards/awards_{season}.html"

    # Return empty dataframe - Current Season will have no mvp voting data
    if (season == CURRENT_SEASON):
        return pd.DataFrame(columns=['rank', 'Player', 'Age', 'Team', 'First', 'Pts Won', 'Pts Max', 'Share', 'G', 'MP', 'PTS',
                                     'TRB', 'AST', 'STL', 'BLK', 'FG%', '3P%', 'FT%', 'WS', 'WS/48'])
    
    tables = retrieve_tables_from_url(url)

    # Required data is kept in first table

    df = tables[0]

    df.reset_index(drop=True, inplace=True)

    return flatten_columns(df)


def retrieve_tables_from_url(url: str) -> List[pd.DataFrame]:
    """
    Retrieve all HTML tables from a specified URL
    
    Params:
        url (str): The URL of webpage containing HTML tables.

    Returns:
        list[pd.DataFrame]: List of tables retrieved from the webpage

    Raises:
        ValueError: If no tables are found at the specified URL
    """    
    
    tables = pd.read_html(url)

    if not tables:
        raise ValueError(f'No tables found in url: {url}')
    
    return tables


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flattens column names in a DataFrame to eliminate tuple values (e.g. ('Voting', 'Pts') -> 'Voting_Pts')"""

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col) for col in df.columns.values]
    return df