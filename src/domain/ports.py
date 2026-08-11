from typing import Any, Dict, List, Protocol, runtime_checkable

import pandas as pd

# @runtime_checkable so `isinstance(instance, SomePort)` works -- used by
# get_instance() (src/utils/dynamic_loader.py) to verify a class configured in
# config.yaml actually implements the port it's wired in as, at construction
# time. NOTE: this is a structural check (does the named method exist?), not
# a signature/type check and not async-vs-sync aware -- Python's
# runtime_checkable Protocols can't verify parameter types or return types.
# It catches "this class doesn't implement retrieve() at all" (a config typo
# pointing at the wrong class), not "retrieve() has a different signature."


@runtime_checkable
class FetcherPort(Protocol):
    """Contract implemented by src/infrastructure/fetcher/* adapters."""

    async def retrieve(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
        batch_config: Dict[str, Any] = None,
    ) -> pd.DataFrame: ...


@runtime_checkable
class CleanerPort(Protocol):
    """Contract implemented by src/infrastructure/cleaner/* adapters."""

    def clean(self, data: pd.DataFrame) -> List[Dict[str, Any]]: ...


@runtime_checkable
class RepositoryPort(Protocol):
    """Contract implemented by the DB access adapter (src/infrastructure/repository)."""

    def connect(self) -> None: ...

    def insert_data(self, data: List[Dict[str, Any]], schema: str, table: str) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class SymbolSourcePort(Protocol):
    """Contract implemented by src/infrastructure/loader/* adapters."""

    def load_symbols(self) -> Dict[str, str]: ...


@runtime_checkable
class QueryPort(Protocol):
    """
    Contract the GraphQL API layer (src/api) depends on. Thin pass-through to
    OhlcvRepository's read methods -- no business logic lives behind this
    port, it exists so the API layer depends on an interface the domain
    defines rather than importing the infrastructure repository directly.
    """

    def get_ohlcv_data(
        self, start_date: str, end_date: str, symbols: List[str] = None
    ) -> List[Dict[str, Any]]: ...

    def get_symbols(self) -> List[str]: ...

    def get_latest_date(self) -> Any: ...

    def get_earliest_date(self) -> Any: ...
