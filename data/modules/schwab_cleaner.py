import pandas as pd
from enum import Enum
from cleaner import Cleaner
from typing import Dict, Any, List
import logging

class RequiredFieldsEquity(Enum):
    """
    Attributes:
        SYMBOL (str): The symbol or identifier for the asset.
        OPEN (str): The opening price.
        HIGH (str): The highest price.
        LOW (str): The lowest price.
        CLOSE (str): The closing price.
        VOLUME (str): The trading volume.
        DATETIME (str): The timestamp of the data row.
    """
    SYMBOL = "symbol"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"
    DATETIME = "datetime"

class RequiredFieldsOption(Enum):
    """
    Attributes:
        CALL (str): 0 for put, 1 for call.
        STRIKE (str): The strike price of the option.
        PREMIUM (str): The current mid price of the option.
        DTE (str): Days until the option expires.
        IV (str): Current implied volatility of the option.
        TYPE (str): If option is call or put (sounds like duplicate, maybe remove later).
        THEORETICAL_VALUE (str): The current theoretical Black Scholes price of the option.
        ND2: Delta 2. Probability that the market is pricing in that option will expire in the money (can be very useful for certain calculations)
        DELTA: Current delta of option. Derivative of value with respect to price.
        GAMMA: Current gamma of option. Derivative of delta with respect to price.
        THETA: Current theta of option. Derivative of value with respect to time.
        VEGA: Current vega of option. Derivative of value with respect to change in volatility.
        DATETIME: Datetime in which data was parsed. Every row should have same value for a given df
    """
    CALL = "call"
    STRIKE = "strike"
    PREMIUM = "premium"
    DTE = "dte"
    IV = "iv"
    TYPE = "type"
    THEORETICAL_VALUE = "theo_value"
    ND2 = "nd2"
    DELTA = "delta"
    GAMMA = "gamma"
    THETA = "theta"
    VEGA = "vega"
    DATETIME = "Datetime"


class SchwabCleaner(Cleaner): # avaliable assets types current are OPTION and EQUITY
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config: Dict[str, Any] = config
        self.logger: logging.Logger = logging.getLogger("SchwabCleaner")
        print(self.config)

    def clean(self):
        pass
    def validate_fields(self, data: pd.DataFrame,) -> pd.DataFrame:
        if self.config["provider"]["asset"] == "OPTION":
            required_fields: List[str] = [field.value for field in RequiredFieldsOption]
            missing_fields: List[str] = [field for field in required_fields if field not in data.columns]
            if missing_fields:
                logging.error(f"Missing required fields: {missing_fields}")
                raise ValueError(f"Missing required fields: {missing_fields}")
        elif self.config["provider"]["asset"] == "EQUITY":
            required_fields: List[str] = [field.value for field in RequiredFieldsOption]
            missing_fields: List[str] = [field for field in required_fields if field not in data.columns]
            if missing_fields:
                logging.error(f"Missing required fields: {missing_fields}")
                raise ValueError(f"Missing required fields: {missing_fields}")
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
from utils.dynamic_loader import load_config
config = load_config()
ye = SchwabCleaner(config)



