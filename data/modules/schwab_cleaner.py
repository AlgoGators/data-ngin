import pandas as pd

from cleaner import Cleaner


class SchwabCleaner(Cleaner):
    def validate_fields(self, data: pd.DataFrame) -> pd.DataFrame:
