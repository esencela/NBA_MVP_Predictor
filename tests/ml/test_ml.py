import pytest # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from source.ml.preprocessing import add_features


class TestPreprocessing():
    def test_add_features(self):
        df = pd.DataFrame({
            'Player': ['Player A', 'Player B', 'Player C', 'Player D'],
            'Share': [0.1, 0.0, 0.5, 0.0]
        })

        df = add_features(df)

        assert 'has_votes' in df.columns
        assert df['has_votes'].tolist() == [1, 0, 1, 0]