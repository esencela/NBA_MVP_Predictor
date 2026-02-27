import pandas as pd # pyright: ignore[reportMissingModuleSource]
from bs4 import BeautifulSoup # pyright: ignore[reportMissingImports]
from typing import List
from pathlib import Path
from datetime import datetime
import logging
from source.config.settings import CURRENT_SEASON

logger = logging.getLogger(__name__)

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
        dict: Dictionary that holds all datasets and last update time for source data:
            {
                'per_game': pd.DataFrame,
                'advanced': pd.DataFrame,
                'team': {
                    'east': pd.DataFrame,
                    'west': pd.DataFrame
                },
                'mvp': pd.DataFrame,
                'last_update': datetime
            }
    """

    per_game = extract_per_game_season_data(season)
    advanced = extract_advanced_season_data(season)
    team = extract_team_season_data(season)
    mvp = extract_mvp_vote_data(season)

    dict_freshness = {
        'per_game': per_game['last_update'],
        'advanced': advanced['last_update'],
        'team': team['last_update'],
        'mvp': mvp['last_update']       
    }

    if season == CURRENT_SEASON:
        # Current season will have no mvp voting data, so only check freshness of other datasets
        keys = ['per_game', 'advanced', 'team']
    else:
        keys = ['per_game', 'advanced', 'team', 'mvp']

    # Raise ValueError if any datasets have different update times, indicative of failed extraction or stale data
    update_times = set(dict_freshness[k] for k in keys)

    if len(update_times) > 1:
        logger.error('Datasets have different update times: %s', dict_freshness)
        raise ValueError(f'Datasets have different update times: {dict_freshness}')
    
    logger.info(f'Extracted data for {season} season')

    return {
        'per_game': per_game['data'],
        'advanced': advanced['data'],
        'team': {
            'east': team['east'],
            'west': team['west']
        },
        'mvp': mvp['data'],
        'last_update': dict_freshness['per_game']
    }


def extract_per_game_season_data(season: int) -> dict:
    """
    Extract NBA per-game player statistics for a given season from locally saved Basketball Reference html page.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        dict: Dictionary containing raw per-game player statistics and last update time for the given season.
            {
                'data': pd.DataFrame,
                'last_update': datetime
            }
    """

    url = Path(f'/opt/airflow/html_snapshots/NBA_{season}_per_game.html')

    tables = retrieve_tables_from_url(url)

    # Required data is kept in first table
    df = tables[0]

    df['player_id'] = retrieve_player_ids(url, 'per_game_stats')

    # Retrieves last update time of local html snapshot
    last_update = retrieve_last_update_time(url)

    logger.info('Stats last updated on %s', last_update)

    # Last row contains unnecessary data, drop it from table
    df.drop(df.tail(1).index, inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info('Extracted per-game data for %s season with %s rows', season, len(df)) 

    return {
        'data': df,
        'last_update': last_update
    }


def extract_advanced_season_data(season: int) -> dict:
    """
    Extract NBA advanced player statistics for a given season from locally saved Basketball Reference html page.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        dict: Dictionary containing raw advanced player statistics and last update time for the given season.
            {
                'data': pd.DataFrame,
                'last_update': datetime
            }
    """

    url = Path(f'/opt/airflow/html_snapshots/NBA_{season}_advanced.html')

    tables = retrieve_tables_from_url(url)

    # Required data is kept in first table
    df = tables[0]

    df['player_id'] = retrieve_player_ids(url, 'advanced')

    # Retrieve last update time of local html snapshot
    last_update = retrieve_last_update_time(url)

    logger.info('Stats last updated on %s', last_update)

    # Last row contains unnecessary data, drop it from table
    df.drop(df.tail(1).index, inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info('Extracted advanced data for %s season with %s rows', season, len(df)) 
    
    return {
        'data': df,
        'last_update': last_update
    }


def extract_team_season_data(season: int) -> dict:
    """
    Extract NBA team statistics for a given season from locally saved Basketball Reference html page.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        dict: Dictionary containing raw team statistics and last update time for the given season (east, west).
            {
                'east': pd.DataFrame,
                'west': pd.DataFrame,
                'last_update': datetime
            }
    """

    url = Path(f'/opt/airflow/html_snapshots/NBA_{season}_standings.html')

    tables = retrieve_tables_from_url(url)

    # Required data is split between first two tables
    df_east = tables[0]
    df_west = tables[1]

    df_east.reset_index(drop=True, inplace=True)
    df_west.reset_index(drop=True, inplace=True)

    # Retrieve last update time of local html snapshot
    last_update = retrieve_last_update_time(url)

    logger.info('Stats last updated on %s', last_update)

    logger.info('Extracted team data for %s season with %s rows for east and %s rows for west', 
                season, 
                len(df_east), 
                len(df_west))

    return {
        'east': df_east,
        'west': df_west,
        'last_update': last_update
    }


def extract_mvp_vote_data(season: int) -> pd.DataFrame:
    """
    Extract NBA MVP voting data for a given season from Basketball Reference.

    Params:
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        pd.DataFrame: Raw MVP voting data for the given season.
    """

    url = Path(f'/opt/airflow/html_snapshots/awards_{season}.html')

    # Current Season will have no mvp voting data, return empty dataframe
    if (season == CURRENT_SEASON):
        logger.warning('No MVP voting data available for current season, returning empty dataframe')

        return { 
            'data': pd.DataFrame(columns=['rank', 'Player', 'Age', 'Team', 'First', 'Pts Won', 'Pts Max', 'Share', 'G', 'MP', 'PTS',
                                     'TRB', 'AST', 'STL', 'BLK', 'FG%', '3P%', 'FT%', 'WS', 'WS/48', 'player_id_']),
            'last_update': datetime.now() 
        }
    
    tables = retrieve_tables_from_url(url)

    # Required data is kept in first table
    df = tables[0]

    df['player_id'] = retrieve_player_ids(url, 'mvp')

    # Retrieve last update time of local html snapshot
    last_update = retrieve_last_update_time(url)

    logger.info('MVP voting data last updated on %s', last_update)

    df.reset_index(drop=True, inplace=True)

    logger.info('Extracted MVP voting data for %s season with %s rows', season, len(df))

    return {
        'data': flatten_columns(df),
        'last_update': last_update
    }


def retrieve_tables_from_url(url: str) -> List[pd.DataFrame]:
    """
    Retrieve all tables from a html page.
    
    Params:
        url (str): The path of HTML file containing tables.

    Returns:
        list[pd.DataFrame]: List of tables retrieved from HTML.

    Raises:
        ValueError: If no tables are found at the specified path.
    """    
    
    tables = pd.read_html(url)

    if not tables:
        logger.error('No tables found at %s', url)
        raise ValueError(f'No tables found at {url}')
    
    logger.info('Retrieved %s tables from %s', len(tables), url)
    
    return tables


def retrieve_player_ids(url: str, table_id: str) -> List[str]:
    """
    Retrieve player IDs from html page.
    
    Params:
        url (str): The path of HTML file containing the target table.
        table_id (str): The HTML id attribute of the target table.

    Returns:
        list[str]: List of player IDs retrieved from the specified table.
    """

    content = url.read_text()
    soup = BeautifulSoup(content, 'html.parser')

    table = soup.find('table', id=table_id).find('tbody')
    player_ids = []   

    # Player IDs are stored in the 'data-append-csv' attribute of the first 'td' element in each row of the table
    for row in table.find_all('tr'):
        cell = row.find("td", {"data-append-csv": True})  
        if cell:
            player_id = cell['data-append-csv']

        player_ids.append(player_id)

    return player_ids


def retrieve_last_update_time(url: str):
    """
    Retrieve last update time from basketball reference page.
    
    Params:
        url (str): The URL of HTML file containing the last update time.
    """

    content = url.read_text(encoding='utf-8')
    soup = BeautifulSoup(content, 'html.parser')

    # Last update time is stored in a strong tag with the text 'Site Last Updated:'
    tag = soup.find('strong', string='Site Last Updated:')

    if tag and tag.parent:
        update_string = tag.parent.text.replace('Site Last Updated: ', '')
    else:
        raise ValueError(f'Could not find last update time in url: {url}')
    
    cleaned_string = update_string.strip().replace('  ', ' ')
    
    # Convert string to datetime object
    dt = datetime.strptime(cleaned_string, '%A, %B %d,  %I:%M%p')

    # Replace default year with current year
    dt = dt.replace(year=datetime.now().year)

    # If time is in future, assume previous year
    if dt > datetime.now():
        dt = dt.replace(year=datetime.now().year - 1)
    
    return dt


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flattens column names in a DataFrame to eliminate tuple values (e.g. ('Voting', 'Pts') -> 'Voting_Pts')"""

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col) for col in df.columns.values]
        
    return df