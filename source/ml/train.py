from source.ml.model import LGBMModel
from source.ml.preprocessing import add_features
from source.db.connection import get_data
from source.config.settings import (
    CURRENT_SEASON,
    MODEL_PATH
)


def train_model():
    query = f'SELECT * FROM stats.player_features WHERE "Season" < {CURRENT_SEASON}'

    df = get_data(query)

    df = add_features(df)

    feature_columns = ['MP', 'PTS', 'AST', 'TRB', 'STL', 'BLK', 'TS%', 'PER', 'WS', 'BPM', 'VORP', 'USG%', 'W/L%', 'VORP_W/L']

    X_train = df[feature_columns]
    y_class = df['has_votes']
    y_regr = df['Share']

    model = LGBMModel()
    model.fit(X_train, y_class, X_train, y_regr)
    model.save(MODEL_PATH)