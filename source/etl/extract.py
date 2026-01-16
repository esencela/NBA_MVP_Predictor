import requests # pyright: ignore[reportMissingModuleSource]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from typing import NamedTuple
import time

class ExtractedData(NamedTuple):
    per_game: pd.DataFrame
    advanced: pd.DataFrame
    team: pd.DataFrame
    mvp: pd.DataFrame


def extract(season: int) -> ExtractedData:
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
        ExtractedData: NamedTuple that holds all dataframes (per_game, advanced, team, mvp)
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

    return ExtractedData(per_game, advanced, team, mvp)


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


def extract_team_season_data(season: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract NBA team statistics for a given season from Basketball Reference.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Raw team statistics for the given season (East, West).
    """

    url = f'https://www.basketball-reference.com/leagues/NBA_{season}_standings.html'
    tables = retrieve_tables_from_url(url)

    # Required data is split between first two tables
    df_east = tables[0]
    df_west = tables[1]

    df_east.reset_index(drop=True, inplace=True)
    df_west.reset_index(drop=True, inplace=True)

    return df_east, df_west


def extract_mvp_vote_data(season: int) -> pd.DataFrame:
    """
    Extract NBA MVP voting data for a given season from Basketball Reference.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        pd.DataFrame: Raw MVP voting data for the given season.
    """

    url = f"https://www.basketball-reference.com/awards/awards_{season}.html"
    tables = retrieve_tables_from_url(url)

    # Required data is kept in first table
    df = tables[0]

    df.reset_index(drop=True, inplace=True)

    return df


def retrieve_tables_from_url(url: str) -> list[pd.DataFrame]:
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
