#17

import unittest
from unittest.mock import patch, MagicMock
from data.modules.data_staleness import check_data_staleness_and_gaps
from datetime import datetime, timedelta

class TestDataQualityCheck(unittest.TestCase):
    @patch('data_quality_check.load_config')
    @patch('data_quality_check.PostgresHook')
    def test_check_data_staleness_and_gaps(self, MockPostgresHook, MockLoadConfig):
        # Mock configuration
        mock_config = {
            'database': {
                'raw_table': 'market_data'
            },
            'time_range': {'start': '2024-01-01', 'end': '2024-12-31'}
        }
        MockLoadConfig.return_value = mock_config
        
        # Mock PostgresHook and database queries
        mock_hook_instance = MagicMock()
        MockPostgresHook.return_value = mock_hook_instance
        
        # Test case for data staleness - Latest timestamp is 2 days ago
        mock_hook_instance.get_first.return_value = [datetime.now() - timedelta(days=2)]
        with patch('data_quality_check.logging') as mock_logging:
            check_data_staleness_and_gaps()
            # Check if "Data is stale" warning was logged
            mock_logging.warning.assert_any_call("Data is stale.")

        # Test case for data staleness - Latest timestamp is within the last day
        mock_hook_instance.get_first.return_value = [datetime.now() - timedelta(hours=12)]
        with patch('data_quality_check.logging') as mock_logging:
            check_data_staleness_and_gaps()
            # Check if "Data is not stale" info was logged
            mock_logging.info.assert_any_call("Data is not stale.")

        # Test case for gaps in data
        mock_hook_instance.get_records.return_value = [
            [datetime(2024, 3, 1)],
            [datetime(2024, 3, 3)],  # Gap between March 1 and March 3
            [datetime(2024, 3, 4)]
        ]
        with patch('data_quality_check.logging') as mock_logging:
            check_data_staleness_and_gaps()
            # Check if "Data gap detected" warning was logged
            mock_logging.warning.assert_any_call("Data gap detected between 2024-03-01 00:00:00 and 2024-03-03 00:00:00")

        # Test case for no data in the table (empty data set)
        mock_hook_instance.get_records.return_value = []
        with patch('data_quality_check.logging') as mock_logging:
            check_data_staleness_and_gaps()
            # Check if "No records found" warning was logged
            mock_logging.warning.assert_any_call("No records found to check for gaps.")

if __name__ == '__main__':
    unittest.main()
