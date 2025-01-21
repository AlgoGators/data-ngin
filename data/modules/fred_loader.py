import yaml
import os
from typing import Dict, Any
from data.modules.loader import Loader

class FREDLoader(Loader):
    """
    Loads dataset mappings and metadata for FRED data.

    Attributes:
        series_mapping (str): Path to the YAML file containing dataset mappings.
        series_metadata (str): Path to the YAML file containing metadata mappings.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the FREDLoader with a configuration.

        Args:
            config (Dict[str, Any]): Configuration dictionary.
        """
        super().__init__(config=config)
        try:
            self.series_mapping = config["loader"]["series_mapping"]
            self.series_metadata = config["loader"]["series_metadata"]
        except KeyError as e:
            raise KeyError(f"Missing configuration key: {e}")

    def load_symbols(self) -> Dict[str, Dict[str, Any]]:
        """
        Loads the dataset-to-table and metadata mappings.

        Returns:
            Dict[str, Dict[str, Any]]: A dictionary with dataset information.
        """
        # Load dataset-to-table mapping
        if not os.path.exists(self.series_mapping):
            raise FileNotFoundError(f"Mapping file not found: {self.series_mapping}")

        with open(self.series_mapping, "r") as f:
            mapping = yaml.safe_load(f)

        # Load series metadata
        if not os.path.exists(self.series_metadata):
            raise FileNotFoundError(f"Metadata file not found: {self.series_metadata}")

        with open(self.series_metadata, "r") as f:
            metadata = yaml.safe_load(f)

        # Combine mappings
        result = {}
        for series_name, series_info in mapping.items():
            result[series_name] = {
                "fred_id": series_info["fred_id"],
                "table": series_info["table"],
                "metadata": metadata.get(series_name, {})
            }

        return result
    