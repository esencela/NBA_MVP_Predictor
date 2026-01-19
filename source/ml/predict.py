from source.ml.model import LGBMModel
from source.db.connection import get_data
from source.config.settings import (
    CURRENT_SEASON,
    MODEL_PATH
)


def get_predictions():
    model = LGBMModel.load(MODEL_PATH)

    query = f'SELECT * FROM stats.player_features WHERE "Season" = {CURRENT_SEASON}'

    X_current = get_data(query)

    feature_columns = ['MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK', 'TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%', 'W/L%', 'VORP_W/L']
    id_columns = ['Player', 'Season']

    predictions = X_current[id_columns]

    yhat = model.predict(X_current[feature_columns])

    predictions['Predicted_Share'] = yhat

    return predictions