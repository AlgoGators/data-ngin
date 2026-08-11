from typing import List, Optional

import strawberry

from src.domain.ports import QueryPort


@strawberry.type
class OHLCVBar:
    time: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int


def _row_to_bar(row: dict) -> OHLCVBar:
    return OHLCVBar(
        time=str(row["time"]),
        symbol=row["symbol"],
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=int(row["volume"]),
    )


@strawberry.type
class Query:
    """
    Thin GraphQL surface over QueryPort (OhlcvRepository's read methods).
    Every resolver reads `info.context["repository"]`, which the FastAPI
    context getter (src/api/server.py) populates per-request -- no business
    logic lives here, only translation between GraphQL args and repository calls.
    """

    @strawberry.field
    def ohlcv(
        self,
        info: strawberry.types.Info,
        start_date: str,
        end_date: str,
        symbols: Optional[List[str]] = None,
    ) -> List[OHLCVBar]:
        repository: QueryPort = info.context["repository"]
        rows = repository.get_ohlcv_data(start_date, end_date, symbols=symbols)
        return [_row_to_bar(row) for row in rows]

    @strawberry.field
    def symbols(self, info: strawberry.types.Info) -> List[str]:
        repository: QueryPort = info.context["repository"]
        return repository.get_symbols()

    @strawberry.field
    def latest_date(self, info: strawberry.types.Info) -> Optional[str]:
        repository: QueryPort = info.context["repository"]
        return repository.get_latest_date()

    @strawberry.field
    def earliest_date(self, info: strawberry.types.Info) -> Optional[str]:
        repository: QueryPort = info.context["repository"]
        return repository.get_earliest_date()


schema = strawberry.Schema(query=Query)
