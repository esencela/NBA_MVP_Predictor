from source.ml.model import LGBMModel
from source.db.connection import query_data
from source.config.settings import (
    CURRENT_SEASON,
    MODEL_PATH
)


def get_predictions():
    """
    Predicts MVP vote share using saved model and current season data.

    This function generates predictions for current season data:
    - Loads model from MODEL_PATH
    - Queries PostgreSQL for current season stats
    - Uses model to generate predictions
    - Stores predictions in a DataFrame

    Returns:
        pd.DataFrame: DataFrame holding model predictions along with player and season info.
    """

    model = LGBMModel.load(MODEL_PATH)

    query = f'SELECT * FROM stats.player_features WHERE "Season" = {CURRENT_SEASON}'

    X_current = query_data(query, user='ml')

    feature_columns = ['MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK', 'TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%', 'W/L%', 'VORP_W/L']
    id_columns = ['Season', 'Player']

    predictions = X_current[id_columns]

    yhat = model.predict(X_current[feature_columns])

    predictions['Predicted_Share'] = yhat

    return predictions