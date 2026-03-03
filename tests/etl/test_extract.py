import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
import source.etl.extract as extract


class TestRetrieveTablesFromURL:
    def test_output(self):
        url = Path('tests/data/NBA_2025_per_game.html')

        result = extract.retrieve_tables_from_url(url)

        assert isinstance(result, list)
        assert len(result) > 0


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