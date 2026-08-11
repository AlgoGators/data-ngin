from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from src.domain.services import SymbolRemapper


class ComponentConfig(BaseModel):
    """Base for the loader/fetcher/cleaner/inserter component-selection blocks."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    class_name: str = Field(alias="class")
    module: str


class LoaderConfig(ComponentConfig):
    file_path: Optional[str] = None


class FetcherConfig(ComponentConfig):
    pass


class CleanerConfig(ComponentConfig):
    pass


class InserterConfig(ComponentConfig):
    pass


class ProviderConfig(BaseModel):
    name: str
    asset: str
    dataset: str
    schema_name: str = Field(alias="schema")
    roll_type: str
    contract_type: str

    model_config = ConfigDict(populate_by_name=True)


class DatabaseConfig(BaseModel):
    target_schema: str
    raw_table: str
    table: str


class TimeRangeConfig(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class MissingDataConfig(BaseModel):
    """
    Real bool fields. Pydantic v2 accepts the legacy "True"/"False" strings
    already in config.yaml and coerces them to actual booleans, which is what
    structurally kills the string-compare bug that used to make a real YAML
    `true` a silent no-op (see MissingDataFiller, updated in lockstep to
    check truthiness instead of `== "True"`).
    """

    forward_fill: bool = False
    backward_fill: bool = False
    interpolate: bool = False
    drop_nan: bool = False
    zero_fill: bool = False
    mean_fill: bool = False
    median_fill: bool = False
    custom_fill: bool = False
    custom_value: float = 0


class LoggingConfig(BaseModel):
    level: str = "INFO"


class BatchDownloadingConfig(BaseModel):
    batch: bool = False
    unit: str = "Daily"
    max_units: int = 30


class BackAdjustmentConfig(BaseModel):
    applies_to: str = "FUTURE"


class PipelineConfig(BaseModel):
    """
    Validated, typed contract for config.yaml. `load_config` in
    utils/dynamic_loader.py parses the raw YAML through this model and
    returns config.model_dump(by_alias=True) -- same dict shape every
    existing dict-style config[...] access expects, but with real types
    (e.g. actual bools for missing_data.*) instead of an untyped blob.
    """

    loader: LoaderConfig
    fetcher: FetcherConfig
    cleaner: CleanerConfig
    inserter: InserterConfig
    provider: ProviderConfig
    database: DatabaseConfig
    time_range: TimeRangeConfig = TimeRangeConfig()
    missing_data: MissingDataConfig = MissingDataConfig()
    logging: LoggingConfig = LoggingConfig()
    batch_downloading: BatchDownloadingConfig = BatchDownloadingConfig()
    back_adjustment: BackAdjustmentConfig = BackAdjustmentConfig()
    symbol_remap: Dict[str, str] = Field(default_factory=lambda: dict(SymbolRemapper.DEFAULT_REMAP))
