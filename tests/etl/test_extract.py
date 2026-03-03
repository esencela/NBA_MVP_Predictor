import pytest # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource]
from pathlib import Path
from datetime import datetime
import source.etl.extract as extract


class TestRetrieveTablesFromURL:
    def test_output(self):
        url = Path('tests/data/NBA_2025_per_game.html')

        result = extract.retrieve_tables_from_url(url)

        assert isinstance(result, list)
        assert all(isinstance(table, pd.DataFrame) for table in result)
        assert len(result) > 0


    def test_raises_value_error_on_no_tables(self, tmp_path):
        html = "<html><body><p>No tables here</p></body></html>"
        temp_file = tmp_path / "no_tables.html"
        temp_file.write_text(html, encoding='utf-8')

        with pytest.raises(ValueError, match="No tables found at"):
            extract.retrieve_tables_from_url(temp_file)


class TestRetrievePlayerIDs:
    def test_output(self):
        url = Path('tests/data/NBA_2025_per_game.html')

        result = extract.retrieve_player_ids(url, 'per_game_stats')

        assert isinstance(result, list)
        assert len(result) > 0


class TestRetrieveLastUpdateTime:
    def test_output(self):
        url = Path('tests/data/NBA_2025_per_game.html')

        result = extract.retrieve_last_update_time(url)

        assert isinstance(result, datetime)

    
    def test_raises_value_error_on_no_update_time(self, tmp_path):
        html = '<html><body><p>No update time here</p></body></html>'
        temp_file = tmp_path / 'no_update_time.html'
        temp_file.write_text(html, encoding='utf-8')

        with pytest.raises(ValueError, match='Could not find last update time in url:'):
            extract.retrieve_last_update_time(temp_file)