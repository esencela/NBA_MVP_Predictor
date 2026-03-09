import pytest
import pandas as pd
import source.etl.transform as transform


class TestAddSeasonColumn:
    def test_output(self):
        df = pd.DataFrame({
            'Player': ['Player A', 'Player B'],
            'Team': ['Team X', 'Team Y']
        })

        season = 2025

        result = transform.add_season_column(df, season)

        assert isinstance(result, pd.DataFrame)
        assert 'Season' in result.columns
        assert all(result['Season'] == season)


    def test_raises_value_error_on_non_integer_season(self):
        df = pd.DataFrame({
            'Player': ['Player A', 'Player B'],
            'Team': ['Team X', 'Team Y']
        })

        season = 'test'

        with pytest.raises(ValueError, match='Season value must be an integer'):
            transform.add_season_column(df, season)


class TestCleanPerGameData:
    def test_output(self):
        df = pd.DataFrame({
            'player_id': ['player_a', 'player_b'],
            'Player': ['Player A', 'Player B'],
            'Team': ['Team X', 'Team Y'],
            'G': ['82', '82'],
            'MP': ['35.0', '36.5'],            
            'PTS': ['10.5', '12.3'],
            'AST': ['5.0', '7.2'],
            'TRB': ['4.5', '6.1'],
            'STL': ['1.2', '1.5'],
            'BLK': ['0.5', '0.8'],
            'Test1': ['1.8', '2.3'],
            'Test2': ['2.1', '2.9']
        })

        result = transform.clean_per_game_data(df)

        columns_to_check = ['player_id', 'Player', 'Team', 'G', 'MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK']

        assert isinstance(result, pd.DataFrame)
        assert result['PTS'].dtype == float
        assert all(col in result.columns for col in columns_to_check)
        assert result.isnull().sum().sum() == 0
        
    
    def test_raises_value_error_on_numeric_columns(self):
        df = pd.DataFrame({
            'player_id': ['player_a', 'player_b'],
            'Player': ['Player A', 'Player B'],
            'Team': ['Team X', 'Team Y'],
            'G': ['82', '82'],
            'MP': ['35.0', '36.5'],            
            'PTS': ['10.5', 'invalid'],
            'AST': ['5.0', '7.2'],
            'TRB': ['4.5', '6.1'],
            'STL': ['1.2', '1.5'],
            'BLK': ['0.5', '0.8']
        })

        with pytest.raises(ValueError, match='Error occurred while converting columns to numeric'):
            transform.clean_per_game_data(df)


class TestCleanAdvancedData:
    def test_output(self):
        df = pd.DataFrame({
            'player_id': ['player_a', 'player_b'],
            'TS%': ['50.0', '52.5'],
            'PER': ['15.0', '18.5'],            
            'WS': ['3.5', '5.2'],
            'BPM': ['-1.2', '2.3'],
            'VORP': ['0.5', '1.8'],
            'USG%': ['20.0', '25.5'],
            'Test1': ['1.8', '2.3'],
            'Test2': ['2.1', '2.9']
        })

        result = transform.clean_advanced_data(df)

        columns_to_check = ['player_id', 'TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%']

        assert isinstance(result, pd.DataFrame)
        assert result['PER'].dtype == float
        assert all(col in result.columns for col in columns_to_check)
        assert result.isnull().sum().sum() == 0

    
    def test_raises_value_error_on_numeric_columns(self):
        df = pd.DataFrame({
            'player_id': ['player_a', 'player_b'],
            'TS%': ['50.0', 'invalid'],
            'PER': ['15.0', '18.5'],            
            'WS': ['3.5', '5.2'],
            'BPM': ['-1.2', '2.3'],
            'VORP': ['0.5', '1.8'],
            'USG%': ['20.0', '25.5']
        })

        with pytest.raises(ValueError, match='Error occurred while converting columns to numeric'):
            transform.clean_advanced_data(df)


class TestCleanTeamData:
    def test_output(self):
        df_east = pd.DataFrame({
            'Eastern Conference': ['Boston Celtics', 'Milwaukee Bucks'],
            'W/L%': ['0.52', '0.60'],
            'Test1': ['32', '22'],
            'Test2': ['0.610', '0.730']
        })

        df_west = pd.DataFrame({
            'Western Conference': ['Los Angeles Lakers', 'Golden State Warriors'],
            'W/L%': ['0.48', '0.55'],
            'Test1': ['28', '30'],
            'Test2': ['0.590', '0.680']
        })

        dict_team = {
            'east': df_east,
            'west': df_west
        }

        result = transform.clean_team_data(dict_team)

        columns_to_check = ['Team', 'W/L%']

        assert isinstance(result, pd.DataFrame)
        assert result['W/L%'].dtype == float
        assert all(col in result.columns for col in columns_to_check)
        assert result.isnull().sum().sum() == 0


    def test_raises_key_error_on_unrecognized_team(self):
        df_east = pd.DataFrame({
            'Eastern Conference': ['Team X', 'Team Y'],
            'W/L%': ['0.52', '0.60']
        })

        df_west = pd.DataFrame({
            'Western Conference': ['Team Z', 'Team W'],
            'W/L%': ['0.48', '0.55']
        })

        dict_team = {
            'east': df_east,
            'west': df_west
        }

        with pytest.raises(ValueError, match='Team name not recognized'):
            transform.clean_team_data(dict_team)


    def test_raises_value_error_on_numeric_columns(self):
        df_east = pd.DataFrame({
            'Eastern Conference': ['Boston Celtics', 'Milwaukee Bucks'],
            'W/L%': ['invalid', '0.60']
        })

        df_west = pd.DataFrame({
            'Western Conference': ['Los Angeles Lakers', 'Golden State Warriors'],
            'W/L%': ['0.48', '0.55']
        })

        dict_team = {
            'east': df_east,
            'west': df_west
        }

        with pytest.raises(ValueError, match='Error occurred while converting W/L column to numeric'):
            transform.clean_team_data(dict_team)


class TestCleanMVPVoteData:
    def test_output(self):
        df = pd.DataFrame({
            'rank': ['1', '2'],
            'Player': ['Player A', 'Player B'],
            'Age': ['25', '32'],
            'Team': ['Team X', 'Team Y'],
            'First': ['50', '30'],
            'Pts Won': ['800', '550'],
            'Pts Max': ['1000', '1000'],
            'Share': ['0.800', '0.550'],
            'G': ['82', '82'],
            'MP': ['35.0', '36.5'],
            'PTS': ['10.5', '12.3'],
            'TRB': ['4.5', '6.1'],
            'AST': ['5.0', '7.2'],
            'STL': ['1.2', '1.5'],
            'BLK': ['0.5', '0.8'],
            'FG%': ['45.0', '47.5'],
            '3P%': ['35.0', '38.5'],
            'FT%': ['80.0', '85.5'],
            'WS': ['3.5', '5.2'],
            'WS/48': ['0.100', '0.150'],
            'player_id': ['player_a', 'player_b']
        })

        result = transform.clean_mvp_vote_data(df)

        columns_to_check = ['player_id', 'Share']

        assert isinstance(result, pd.DataFrame)
        assert result['Share'].dtype == float
        assert all(col in result.columns for col in columns_to_check)
        assert result.isnull().sum().sum() == 0


    def test_raises_value_error_on_numeric_columns(self):
        df = pd.DataFrame({
            'rank': ['1', '2'],
            'Player': ['Player A', 'Player B'],
            'Age': ['25', '32'],
            'Team': ['Team X', 'Team Y'],
            'First': ['50', '30'],
            'Pts Won': ['800', '550'],
            'Pts Max': ['1000', '1000'],
            'Share': ['0.800', 'Invalid'],
            'G': ['82', '82'],
            'MP': ['35.0', '36.5'],
            'PTS': ['10.5', '12.3'],
            'TRB': ['4.5', '6.1'],
            'AST': ['5.0', '7.2'],
            'STL': ['1.2', '1.5'],
            'BLK': ['0.5', '0.8'],
            'FG%': ['45.0', '47.5'],
            '3P%': ['35.0', '38.5'],
            'FT%': ['80.0', '85.5'],
            'WS': ['3.5', '5.2'],
            'WS/48': ['0.100', '0.150'],
            'player_id': ['player_a', 'player_b']
        })

        with pytest.raises(ValueError, match='Error occurred while converting Share column to numeric'):
            transform.clean_mvp_vote_data(df)

    
    def test_raises_value_error_on_incorrect_columns(self):
        df = pd.DataFrame({
            'rank': ['1', '2'],
            'Player': ['Player A', 'Player B'],
            'Age': ['25', '32'],
            'Team': ['Team X', 'Team Y'],
            'First': ['50', '30'],
            'Pts Won': ['800', '550'],
            'Pts Max': ['1000', '1000'],
            # Missing Share column
            'G': ['82', '82'],
            'MP': ['35.0', '36.5'],
            'PTS': ['10.5', '12.3'],
            'TRB': ['4.5', '6.1'],
            'AST': ['5.0', '7.2'],
            'STL': ['1.2', '1.5'],
            'BLK': ['0.5', '0.8'],
            'FG%': ['45.0', '47.5'],
            '3P%': ['35.0', '38.5'],
            'FT%': ['80.0', '85.5'],
            'WS': ['3.5', '5.2'],
            'WS/48': ['0.100', '0.150'],
            'player_id': ['player_a', 'player_b']
        })

        with pytest.raises(ValueError, match='Error occurred while renaming columns'):
            transform.clean_mvp_vote_data(df)


class TestImputeWinRate:
    def test_output(self):
        df = pd.DataFrame({
            'player_id': ['player_a', 'player_b'],
            'Team': ['Team X', 'Team Y'],
            'W/L%': [0.52, 0.60],
            'G': [82, 82]
        })

        result = transform.impute_win_rate(df)

        assert isinstance(result, pd.DataFrame)
        assert 'W/L%' in result.columns
        assert result['W/L%'].dtype == float

    
    def test_calculation(self):
        df = pd.DataFrame({
            'player_id': ['player_a', 'player_b', 'player_b', 'player_b'],
            'Team': ['Team X', '2TM', 'Team Y', 'Team Z'],
            'W/L%': [0.52, None, 0.60, 0.50],
            'G': [82, 82, 41, 41]
        })

        result = transform.impute_win_rate(df)

        assert result.loc[result['player_id'] == 'player_b', 'W/L%'].iloc[0] == 0.55


class TestScaleFeatures:
    def test_output(self):
        df = pd.DataFrame({
            'player_id': ['player_a', 'player_b'],
            'MP': [35.0, 36.5],
            'PTS': [10.5, 12.3],
            'AST': [5.0, 7.2],
            'TRB': [4.5, 6.1],
            'STL': [1.2, 1.5],
            'BLK': [0.5, 0.8],
            'TS%': [50.0, 52.5],
            'PER': [15.0, 18.5],
            'WS': [3.5, 5.2],
            'BPM': [-1.2, 2.3],
            'VORP': [0.5, 1.8],
            'USG%': [20.0, 25.5],
            'VORP_W/L': [0.26, 1.08]
        })

        result = transform.scale_features(df)

        columns_to_check = ['MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK', 'TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%', 'VORP_W/L']

        assert isinstance(result, pd.DataFrame)
        assert all(col in result.columns for col in columns_to_check)

        means = result[columns_to_check].mean().round(6)
        stds = result[columns_to_check].std(ddof=0).round(6)

        for mean in means:
            assert abs(mean) < 1e-6
        for std in stds:
            assert abs(std - 1) < 1e-6