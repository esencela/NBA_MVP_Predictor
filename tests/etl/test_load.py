import pytest # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from sqlalchemy import create_engine # pyright: ignore[reportMissingImports]
from unittest.mock import patch
from source.etl.load import load_to_database

class TestLoadToDatabase:
    def test_load_to_database(self, tmp_path):
        # Create a temporary SQLite database for testing
        db_path = tmp_path / "test.db"
        engine = create_engine(f'sqlite:///{db_path}')

        df = pd.DataFrame({
            'Player': ['Alice', 'Bob', 'Charlie'],
            'Points': [10, 20, 30]
        })

        with patch('source.etl.load.get_engine', return_value=engine):
            load_to_database(df, user='etl', table_name='test_table', schema=None, append=True)

            result = pd.read_sql('SELECT * FROM test_table', engine)
            pd.testing.assert_frame_equal(df, result)