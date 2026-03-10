import pytest # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.ml.preprocessing import add_features
from source.ml.model import LGBMModel


class TestPreprocessing():
    def test_add_features(self):
        df = pd.DataFrame({
            'Player': ['Player A', 'Player B', 'Player C', 'Player D'],
            'Share': [0.1, 0.0, 0.5, 0.0]
        })

        df = add_features(df)

        assert 'has_votes' in df.columns
        assert df['has_votes'].tolist() == [1, 0, 1, 0]


class TestModel():
    @pytest.fixture()
    def sample_data(self):
        X = pd.DataFrame({
            'MP': [1000, 500, 2000, 300],
            'PTS': [500, 100, 1500, 50],
            'AST': [200, 50, 400, 20],
        })

        y_class = pd.Series([1, 0, 1, 0])
        y_regr = pd.Series([0.1, 0.0, 0.5, 0.0])

        return X, y_class, y_regr


    def test_fit(self, sample_data):

        X, y_class, y_regr = sample_data

        model = LGBMModel()
        model.fit(X, y_class, y_regr)

        assert model.classifier is not None
        assert model.regressor is not None

    
    def test_predict(self, sample_data):
        X, y_class, y_regr = sample_data

        model = LGBMModel()
        model.fit(X, y_class, y_regr)

        yhat = model.predict(X)

        assert len(yhat) == len(X)

    
    def test_save_and_load(self, sample_data, tmp_path):
        X, y_class, y_regr = sample_data

        model = LGBMModel()
        model.fit(X, y_class, y_regr)

        model_path = tmp_path / 'model.pkl'
        model.save(model_path)

        assert model_path.exists()

        loaded_model = LGBMModel.load(model_path)

        yhat = model.predict(X)
        yhat_loaded = loaded_model.predict(X)
        assert (yhat == yhat_loaded).all()

