import yaml
import os
import logging
import psycopg2
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class Loader(ABC):
    """
    Abstract base class for loaders that handle loading and validating symbols
    from various sources (e.g., CSV, JSON, APIs) based on configuration settings.
    
    Attributes:
        config (Dict[str, Any]): Configuration settings loaded from config.yaml.
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the Loader with a specified configuration file.
        
        Args:
            config (str): Config settings.
        """
        self.config: Dict[str, Any] = config

    @abstractmethod
    def load_symbols(self) -> Dict[str, str]:
        """
        Abstract method for loading symbols from a specific data source.
        This method must be implemented by subclasses.
        
        Returns:
            Dict[str, str]: A dictionary mapping each symbol to its asset type.
        """
        pass

#9
    def validate_data_quality(self, db_conn) -> bool:
        """
        Perform data quality checks using SQL queries.
        
        Args:
            db_conn (psycopg2.connection): The database connection to execute queries.
        
        Returns:
            bool: True if all checks pass, False otherwise.
        """
        try:
            cursor = db_conn.cursor()

            # 1. Check for null values in the symbol or asset_type columns
            cursor.execute("SELECT COUNT(*) FROM symbols WHERE symbol IS NULL OR asset_type IS NULL")
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                self.logger.error(f"Data quality check failed: {null_count} rows have NULL values in symbol or asset_type columns.")
                return False
            
            # 2. Check for zero values in the data columns (replace 'value_column' with actual column name)
            cursor.execute("SELECT COUNT(*) FROM symbols WHERE value_column = 0")
            zero_count = cursor.fetchone()[0]
            if zero_count > 0:
                self.logger.error(f"Data quality check failed: {zero_count} rows have zero values in value_column.")
                return False
            
            # 3. Check for missing gaps in time
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT time, LEAD(time) OVER (ORDER BY time) AS next_time
                    FROM symbols
                ) AS time_gaps
                WHERE next_time IS NOT NULL AND EXTRACT(EPOCH FROM (next_time - time)) > 60
            """)
            gap_count = cursor.fetchone()[0]
            if gap_count > 0:
                self.logger.error(f"Data quality check failed: {gap_count} time gaps detected.")
                return False

            self.logger.info("Data quality checks passed.")
            return True
        
        except Exception as e:
            self.logger.error(f"Error during data quality checks: {e}")
            return False
        finally:
            cursor.close()