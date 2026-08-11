import pandas as pd
from typing import Dict, Any
from src.infrastructure.fetcher.databento_fetcher import DatabentoFetcher


class BatchDownloadDatabentoFetcher(DatabentoFetcher):
    """
    A DatabentoFetcher subclass that splits a date range into batches and
    fetches each batch in turn, concatenating the results. `fetch_data` is
    inherited from `DatabentoFetcher` unchanged.
    """

    async def retrieve(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
        batch_config: Dict[str, Any] = None,
    ) -> pd.DataFrame:
        batch_config = batch_config or {}
        # `batch_downloading.batch` still gates batching even when this class
        # is configured -- defaults to True so existing config.yaml (which
        # sets both `fetcher.class: BatchDownloadDatabentoFetcher` and
        # `batch_downloading.batch: true`) keeps behaving the same; setting
        # it to false falls back to a single unbatched fetch instead of the
        # flag being silently ignored.
        if not batch_config.get("batch", True):
            return await self.fetch_data(
                symbol=symbol,
                loaded_asset_type=loaded_asset_type,
                start_date=start_date,
                end_date=end_date,
            )
        return await self.generate_and_fetch_data(
            symbol=symbol,
            loaded_asset_type=loaded_asset_type,
            start_date=start_date,
            end_date=end_date,
            unit=batch_config.get("unit"),
            max_units_allowed=batch_config.get("max_units"),
        )

    async def generate_and_fetch_data(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
        unit: str,
        max_units_allowed: int,
    ) -> pd.DataFrame:
        batches = self.generate_batches(start_date, end_date, unit, max_units_allowed)

        # Collect frames in chronological batch order and concat once at the
        # end. The previous `pd.concat([data, master_df], ...)` inside the
        # loop prepended each new (later) batch before the accumulator --
        # for a multi-batch fetch the result came back chronologically
        # DESCENDING (batch N, batch N-1, ..., batch 1), each batch
        # internally ascending. BackAdjuster.detect_rolls (src/domain/services.py)
        # walks rows assuming ascending time order, so every batch boundary
        # looked like a false "roll" to it, corrupting back-adjusted prices.
        # Collecting into a list also avoids the O(n^2) copy of repeated
        # concat-in-a-loop on a growing frame.
        frames = []
        for batch in batches:
            data = await self.fetch_data(symbol, loaded_asset_type, start_date=batch[0], end_date=batch[1])
            self.logger.info(f"Successfully fetched data for batch {batch[0]} to {batch[1]}.")
            frames.append(data)

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def generate_batches(self, start_date: str, end_date: str, unit: str, max_units_allowed: int):
        """
        Generate time batches based on a specified time unit and maximum units allowed.

        Args:
            start_date (str): The starting date/time (in a format recognized by pd.Timestamp)
            end_date (str): The ending date/time.
            max_units_allowed (int): Maximum number of time units for each batch.
            unit (str): The time unit to use ("daily", "hourly", or "min").

        Returns:
            List of lists containing [batch_start, batch_end] formatted as strings.

        Raises:
            ValueError: If unit is not daily, hourly or min.
            TypeError: If max_units_allowed is not an integer.
        """
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)

        unit_lower = unit.lower()
        if unit_lower == "daily":
            time_unit = "D"
            date_format = "%Y-%m-%d"
        elif unit_lower == "hourly":
            time_unit = "h"
            date_format = "%Y-%m-%d %H:%M:%S"
        elif unit_lower == "min":
            time_unit = "min"
            date_format = "%Y-%m-%d %H:%M:%S"
        else:
            raise ValueError(f"Unsupported time unit: {unit}")

        if type(max_units_allowed) != int:
            raise TypeError("The maximum units allowed must be an integer.")

        delta = pd.Timedelta(max_units_allowed, unit=time_unit)

        batches = []
        current_ts = start_ts

        while current_ts < end_ts:
            batch_end = current_ts + delta
            if batch_end > end_ts:
                batch_end = end_ts
            batches.append([current_ts.strftime(date_format), batch_end.strftime(date_format)])
            current_ts = batch_end

        return batches
