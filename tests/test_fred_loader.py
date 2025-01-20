import unittest
from unittest.mock import patch, mock_open
import yaml
from data.modules.fred_loader import FREDLoader


class TestFREDLoader(unittest.TestCase):
    def setUp(self):
        self.config = {
            "loader": {
                "series_mapping": "/path/to/fred_series_mapping.yaml",
                "series_metadata": "/path/to/fred_series_metadata.yaml"
            }
        }
        self.series_mapping_content = yaml.dump({
            "GDP": "global_macro_data.gdp",
            "Industrial_Production": "global_macro_data.industrial_production"
        })
        self.series_metadata_content = yaml.dump({
            "GDP": {
                "index_name": "Real GDP",
                "metadata": {
                    "description": "Real GDP in the US.",
                    "units": "Billions of Chained 2012 Dollars",
                    "frequency": "Quarterly"
                }
            }
        })

    @patch("builtins.open", new_callable=mock_open, read_data="mocked content")
    @patch("os.path.exists", return_value=True)
    def test_load_symbols_success(self, mock_exists, mock_open_func):
        mock_open_func.side_effect = [
            mock_open(read_data=self.series_mapping_content).return_value,
            mock_open(read_data=self.series_metadata_content).return_value,
        ]

        loader = FREDLoader(config=self.config)
        result = loader.load_symbols()

        expected_result = {
            "GDP": {
                "table": "global_macro_data.gdp",
                "metadata": {
                    "index_name": "Real GDP",
                    "metadata": {
                        "description": "Real GDP in the US.",
                        "units": "Billions of Chained 2012 Dollars",
                        "frequency": "Quarterly"
                    }
                }
            },
            "Industrial_Production": {
                "table": "global_macro_data.industrial_production",
                "metadata": {}
            }
        }

        self.assertEqual(result, expected_result)

    @patch("os.path.exists", return_value=False)
    def test_load_symbols_file_not_found(self, mock_exists):
        loader = FREDLoader(config=self.config)
        with self.assertRaises(FileNotFoundError):
            loader.load_symbols()


if __name__ == "__main__":
    unittest.main()
