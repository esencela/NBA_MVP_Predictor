import numpy as np
import pandas as pd

def add_season_column(df: pd.DataFrame, season: int) -> pd.DataFrame:
    """
    Adds a 'Season' column to a given DataFrame.
    
    Params:
        df (pd.DataFrame): Dataframe to add Season column to.
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        pd.DataFrame: Dataframe with added Season column.    
    """

    df = df.copy()
    df['Season'] = season

    return df


def clean_per_game_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the per-game dataset, only keeping Player, Minutes Played (MP), Points (PTS), Assists (AST), Total Rebounds (TRB), Steals (STL) and Blocks (BLK) columns and dropping rows with null values
    
    Params:
        df (pd.DataFrame): DataFrame holding raw per-game statistics

    Returns:
        pd.DataFrame: Cleaned DataFrame with only the specified columns
    """

    df = df.copy()

    columns_to_keep = ['Season', 'Player', 'MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK']

    df = df[columns_to_keep]

    df.dropna(inplace=True)

    return df


def clean_advanced_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the advanced dataset, only keeping True Shooting Percentage (TS%), Player Efficiency Rating (PER), Win Shares (WS), Box Plus Minus (BPM), Value over Replacement Player (VORP) and Usage Rate (USG%) columns and dropping rows with null values
    
    Params:
        df (pd.DataFrame): DataFrame holding raw advanced statistics

    Returns:
        pd.DataFrame: Cleaned DataFrame with only the specified columns
    """
    df = df.copy()

    columns_to_keep = ['TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%']

    df = df[columns_to_keep]

    df.dropna(inplace=True)

    return df


def merge_player_data(per_game_data: pd.DataFrame, advanced_data: pd.DataFrame) -> pd.DataFrame:

    df = 