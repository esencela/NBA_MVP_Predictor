import pandas as pd # pyright: ignore[reportMissingModuleSource]
from sklearn.preprocessing import StandardScaler # pyright: ignore[reportMissingModuleSource]


def transform_season_data(raw_player: pd.DataFrame, raw_advanced: pd.DataFrame, raw_team: pd.DataFrame, raw_mvp: pd.DataFrame, season: int) -> dict:
    """
    Transform raw NBA data sources into a model-ready, season-specific feature table.

    This function performs the full transformation pipeline for a single NBA season:
    - Cleans raw per-game, advanced, team, and MVP voting datasets
    - Merges all sources into a unified dataset
    - Imputes missing team win rate values
    - Adds a season identifier
    - Engineers domain-specific features
    - Applies feature scaling
    - Returns a consistently ordered feature set for downstream modeling

    Params:
        raw_player (pd.DataFrame): Raw per-game player statistics.
        raw_advanced (pd.DataFrame): Raw advanced player statistics.
        raw_team (pd.DataFrame): Raw team statistics.
        raw_mvp (pd.DataFrame): Raw MVP voting data.
        season (int): NBA season year (e.g. 2024 for the 2023–24 season).

    Returns:
        dict: A dictionary containing a cleaned, enriched, and scaled player-level feature table suitable for training models [features],
                                           and a cleaned dataset of unscaled player stats for reporting [stats]
    """

    player_data = clean_per_game_data(raw_player)
    adv_data = clean_advanced_data(raw_advanced)
    team_data = clean_team_data(raw_team)
    mvp_data = clean_mvp_vote_data(raw_mvp)

    merged_data = merge_data(player_data, adv_data, team_data, mvp_data)
    transformed_data = impute_win_rate(merged_data)
    transformed_data = add_season_column(transformed_data, season)
    enriched_data = build_features(transformed_data)
    enriched_data = scale_features(enriched_data)

    stats_order = ['Season', 'player_id', 'Player', 'Team', 'MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK']
    feature_order = ['Season', 'player_id', 'Player', 'MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK', 'TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%', 'VORP_W/L', 'W/L%', 'Share']

    df_stats = transformed_data[stats_order]
    df_features = enriched_data[feature_order]

    return {
        'features': df_features,
        'stats': df_stats
    }


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
    Cleans the per-game dataset, only keeping Player, Minutes Played (MP), Points (PTS), Assists (AST), Total Rebounds (TRB), Steals (STL) and Blocks (BLK) columns, 
    dropping rows with null values.
    
    Params:
        df (pd.DataFrame): DataFrame holding raw per-game statistics.

    Returns:
        pd.DataFrame: Cleaned DataFrame with only the specified columns.
    """

    df = df.copy()

    columns_to_keep = ['player_id', 'Player', 'Team', 'G', 'MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK']

    df = df[columns_to_keep]

    df.dropna(inplace=True)

    return df


def clean_advanced_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the advanced dataset, only keeping True Shooting Percentage (TS%), Player Efficiency Rating (PER), Win Shares (WS), Box Plus Minus (BPM),
    Value over Replacement Player (VORP) and Usage Rate (USG%) columns and dropping rows with null values.
    
    Params:
        df (pd.DataFrame): DataFrame holding raw advanced statistics.

    Returns:
        pd.DataFrame: Cleaned DataFrame with only the specified columns.
    """
    df = df.copy()

    columns_to_keep = ['player_id', 'TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%']

    df = df[columns_to_keep]

    df.dropna(inplace=True)

    return df


def clean_team_data(dict_team: dict) -> pd.DataFrame:
    """
    Cleans and concatenates the team standings datasets, only keeping Team Name (Team) and Win Rate (W/L%) columns and mapping a Team Code (Code) onto the data.
    
    Params:
        df_team (dict): Dictionary holding Eastern and Western Conference team data (east, west).

    Returns:
        pd.DataFrame: Cleaned and concatenated DataFrame with only the specified columns.
    """
    df_east = dict_team['east'].copy()
    df_west = dict_team['west'].copy()

    filter = df_east['Eastern Conference'].str.contains('Division')
    df_east = df_east[~filter]

    filter = df_west['Western Conference'].str.contains('Division')
    df_west = df_west[~filter]

    # Keep only letters and spaces then strip extra spaces
    df_east['Eastern Conference'] = df_east['Eastern Conference'].str.replace(r"[^A-Za-z\s]+$", '', regex=True).str.strip()
    df_west['Western Conference'] = df_west['Western Conference'].str.replace(r"[^A-Za-z\s]+$", '', regex=True).str.strip()

    df_east = df_east.rename({'Eastern Conference': 'Team'}, axis=1)
    df_west = df_west.rename({'Western Conference': 'Team'}, axis=1)

    columns_to_keep = ['Team', 'W/L%']

    df_east = df_east[columns_to_keep]
    df_west = df_west[columns_to_keep]

    df_ovr = pd.concat([df_east, df_west])

    df_ovr['W/L%'] = df_ovr['W/L%'].astype('float')

    # Return a three letter code for a specified team name
    def team_code(team_name):
        codes = {'Milwaukee Bucks': 'MIL',
                 'Boston Celtics': 'BOS',
                 'Philadelphia 76ers': 'PHI',
                 'Cleveland Cavaliers': 'CLE',
                 'New York Knicks': 'NYK',
                 'Brooklyn Nets': 'BRK',
                 'Miami Heat': 'MIA',
                 'Atlanta Hawks': 'ATL',
                 'Toronto Raptors': 'TOR',
                 'Chicago Bulls': 'CHI',
                 'Indiana Pacers': 'IND',
                 'Washington Wizards': 'WAS',
                 'Orlando Magic': 'ORL',
                 'Charlotte Hornets': 'CHO',
                 'Detroit Pistons': 'DET',
                 'Charlotte Bobcats': 'CHA',
                 'New Jersey Nets': 'NJN',
                 'Denver Nuggets': 'DEN',
                 'Memphis Grizzlies': 'MEM',
                 'Sacramento Kings': 'SAC',
                 'Phoenix Suns': 'PHO',
                 'Los Angeles Clippers': 'LAC',
                 'Golden State Warriors': 'GSW',
                 'Los Angeles Lakers': 'LAL',
                 'Minnesota Timberwolves': 'MIN',
                 'New Orleans Pelicans': 'NOP',
                 'Oklahoma City Thunder': 'OKC',
                 'Dallas Mavericks': 'DAL',
                 'Utah Jazz': 'UTA',
                 'Portland Trail Blazers': 'POR',
                 'Houston Rockets': 'HOU',
                 'San Antonio Spurs': 'SAS',
                 'New Orleans Hornets': 'NOH',
                 'Seattle SuperSonics': 'SEA',
                 'New Orleans/Oklahoma City Hornets': 'NOK'}
        
        return codes[team_name]
    

    df_ovr['Team'] = df_ovr['Team'].map(team_code)

    return df_ovr


def clean_mvp_vote_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the MVP voting dataset, only keeping Player, Team and Vote Share (Share) columns.
    
    Params:
        df (pd.DataFrame): DataFrame holding MVP voting data.

    Returns:
        pd.DataFrame: Cleaned DataFrame with only the specified columns.
    """

    df = df.copy()

    # Rename nested column names
    df = df.set_axis(['rank', 'Player', 'Age', 'Team', 'First', 'Pts Won', 'Pts Max', 'Share', 'G', 'MP', 'PTS',
                              'TRB', 'AST', 'STL', 'BLK', 'FG%', '3P%', 'FT%', 'WS', 'WS/48', 'player_id'], 
                               axis=1)
    
    columns_to_keep = ['player_id', 'Share']

    df = df[columns_to_keep]

    return df


def merge_data(per_game_data: pd.DataFrame, advanced_data: pd.DataFrame, team_data: pd.DataFrame, mvp_data: pd.DataFrame) -> pd.DataFrame:
    """
    Merges all datasets into one dataframe and fills missing values for MVP vote share.

    Params:
        per_game_data (pd.DataFrame): DataFrame holding cleaned per-game player statistics.
        advanced_data (pd.DataFrame): DataFrame holding cleaned advanced player statistics.
        team_data (pd.DataFrame): DataFrame holding cleaned team statistics.
        mvp_data (pd.DataFrame): DataFrame holding cleaned MVP vote share data.

    Returns:
        pd.DataFrame: Merged DataFrame.
    
    """

    df = per_game_data.merge(advanced_data, how='inner', on='player_id')

    df = df.merge(team_data, how='left', on='Team')

    df = df.merge(mvp_data, how='left', on='player_id')

    # Null values are players with zero vote share
    df['Share'] = df['Share'].fillna(0)

    return df


def impute_win_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputes team win percentage for players who have played for multiple teams by computing a games weighted average of their team win rates.

    For players with a digit in their team code, win percentage is calculated using individual team rows weighted by games played. Duplicate rows are removed.

    Params: 
        df (pd.DataFrame): DataFrame holding merged player and team data.

    Returns:
        pd.DataFrame: DataFrame holding merged data and imputed win rate.
    
    """
    df = df.copy()

    # Boolean mask of players that have played for multiple teams
    multi_team_mask = df['Team'].str.contains('\\d', regex=True)

    # Group by player and calculate weighted win rate - Players with one team will be unaffected
    #weighted_team_stats = (df[~multi_team_mask].groupby('player_id')[['player_id', 'Player', 'W/L%', 'G']]
    #.apply(lambda x: (x['W/L%'] * x['G']).sum() / x['G'].sum()).reset_index(name='W/L%_imputed'))

    weighted_team_stats = (df[~multi_team_mask].groupby('player_id')
                           .apply(lambda x: pd.Series({
                               'W/L%_imputed': (x['W/L%'] * x['G']).sum() / x['G'].sum()
                           })))

    # Merge imputed win rate back into DataFrame
    df = df.merge(weighted_team_stats, on='player_id', how='left')
    df.loc[multi_team_mask, 'W/L%'] = df.loc[multi_team_mask, 'W/L%_imputed']

    # Clean DataFrame
    df.drop_duplicates(subset=['player_id'], keep='first', inplace=True) 
    df.drop(columns='W/L%_imputed', inplace=True)
    df['W/L%'] = df['W/L%'].round(3)

    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes additional features for cleaned and merged DataFrames.
    Calculates the product of Value over Replacement Player and Win Percentage to capture the most valuable players on the best teams.
    
    Params:
        df (pd.DataFrame): DataFrame holding merged player and team data. Requires 'W/L%' column to be imputed.

    Returns:
        pd.DataFrame: DataFrame holding merged data and extra features.
    """
    
    df = df.copy()

    # Computer new feature - VORP * Win Percentage
    df['VORP_W/L'] = df['VORP'] * df['W/L%']

    return df


def scale_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms player data using sklearn.StandardScaler() to capture relative season performance and prepare data for statistical models.
    This function should only be used on DataFrames holding statistics from one season.
    
    Params:
        df (pd.DataFrame): DataFrame holding merged data with built features.

    Returns:
        pd.DataFrame: DataFrame holding merged data and scaled features.
    """

    df = df.copy()
    scaled_columns = ['MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK', 'TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%', 'VORP_W/L']

    scaler = StandardScaler()
    scaler.fit(df[scaled_columns])
    df[scaled_columns] = scaler.transform(df[scaled_columns])

    return df