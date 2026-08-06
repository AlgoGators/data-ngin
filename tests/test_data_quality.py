import unittest
from datetime import date
from unittest.mock import MagicMock, patch
from typing import Any, Dict


class TestDataQuality(unittest.TestCase):
    """Unit tests for DataQuality.find_missing_bars with a mocked session."""

    def setUp(self) -> None:
        self.config: Dict[str, Any] = {
            "database": {
                "db_name": "new_algo_data",
                "target_schema": "equities_data",
                "table": "equities",
            }
        }

    @patch("src.modules.data_quality.get_engine")
    def test_find_missing_bars_returns_symbol_date_tuples(self, mock_engine: MagicMock) -> None:
        from src.modules.data_quality import DataQuality

        dq = DataQuality(config=self.config)
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = False
        mock_session.execute.return_value = [("SATS", date(2026, 7, 9)), ("F", date(2026, 7, 29))]
        dq.Session = MagicMock(return_value=mock_session)

        holes = dq.find_missing_bars("equities_data", "equities")

        self.assertEqual(holes, [("SATS", date(2026, 7, 9)), ("F", date(2026, 7, 29))])

    @patch("src.modules.data_quality.get_engine")
    def test_find_missing_bars_passes_since_and_min_symbols(self, mock_engine: MagicMock) -> None:
        from src.modules.data_quality import DataQuality

        dq = DataQuality(config=self.config)
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = False
        mock_session.execute.return_value = []
        dq.Session = MagicMock(return_value=mock_session)

        dq.find_missing_bars("equities_data", "equities", since="2026-01-01", min_symbols=400)

        params = mock_session.execute.call_args[0][1]
        self.assertEqual(params["since"], "2026-01-01")
        self.assertEqual(params["min_symbols"], 400)

    @patch("src.modules.data_quality.get_engine")
    def test_find_missing_bars_empty_result(self, mock_engine: MagicMock) -> None:
        from src.modules.data_quality import DataQuality

        dq = DataQuality(config=self.config)
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = False
        mock_session.execute.return_value = []
        dq.Session = MagicMock(return_value=mock_session)

        self.assertEqual(dq.find_missing_bars("equities_data", "equities"), [])


if __name__ == "__main__":
    unittest.main()
