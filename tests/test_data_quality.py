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

    @patch("src.modules.data_quality.get_engine")
    def test_missing_bars_sql_has_required_structure(self, mock_engine: MagicMock) -> None:
        """Guard the query's structural invariants, not just plumbing.

        A regression that flipped an anti-join to an inner join, dropped the
        span (BETWEEN sp.lo AND sp.hi) clause, or mis-substituted
        {schema}/{table} would return hundreds of thousands of false holes
        against the live table, yet would pass the mocked-execute tests above
        unchanged. This test inspects the actual query text passed to
        session.execute so such a regression fails here instead.
        """
        from src.modules.data_quality import DataQuality

        dq = DataQuality(config=self.config)
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = False
        mock_session.execute.return_value = []
        dq.Session = MagicMock(return_value=mock_session)

        dq.find_missing_bars("equities_data", "equities")

        sql = str(mock_session.execute.call_args[0][0])

        # Anti-join against the data table itself (bar exists -> excluded).
        self.assertIn("LEFT JOIN have h", sql)
        self.assertIn("h.symbol IS NULL", sql)

        # Anti-join against the confirmed-absent cache (already verified -> excluded).
        # Named specifically (not just "LEFT JOIN", which "LEFT JOIN have h"
        # above would already satisfy) so an inner JOIN here -- which would
        # silently zero out every result in production -- fails this test.
        self.assertIn('LEFT JOIN "equities_data".verified_absent_bars v', sql)
        self.assertIn("v.symbol IS NULL", sql)

        # Span clause: restricts candidates to each symbol's own lifetime.
        self.assertIn("BETWEEN sp.lo AND sp.hi", sql)

        # Trading-day threshold.
        self.assertIn("count(DISTINCT symbol) > :min_symbols", sql)

        # Schema/table were actually substituted (quoted, per the
        # data_access.get_latest_date_for precedent), not left as placeholders.
        self.assertIn('"equities_data"."equities"', sql)
        self.assertNotIn("{schema}", sql)
        self.assertNotIn("{table}", sql)


if __name__ == "__main__":
    unittest.main()
