import os
import logging
import asyncio
from typing import Dict, List, Any, AsyncGenerator, Optional

import pandas as pd
import databento as db
from databento.common.enums import Schema, SType

from data.modules.fetcher import Fetcher


class DatabentoFetcher(Fetcher):
    """
    A Fetcher implementation for streaming real-time data from Databento api.
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the DatabentoFetcher with configuration settings.
        
        Args:
            config (Dict[str, Any]): Configuration settings
        """
        super().__init__(config)
        self.api_key: str = os.getenv("DATABENTO_API_KEY")
        if not self.api_key:
            raise ValueError("DATABENTO_API_KEY environment variable isn't set")
        
        # Configuration parameters
        self.provider_config = self.config.get("provider", {})
        self.dataset: str = self.provider_config.get("dataset", "GLBX.MDP3")
        self.schema: str = self.provider_config.get("schema", "OHLCV_1D")
        self.stype_in: str = "parent"  
        
        #Schema map
        self.schema_mapping = {
            "OHLCV_1D": "ohlcv_1d",
            "OHLCV_1H": "ohlcv_1h",
            "OHLCV_1M": "ohlcv_1m",
            "MBO": "mbo",
            "TRADES": "trades"
        }
        
        #Connection settings
        self.should_reconnect: bool = True
        self.retry_interval: int = self.config.get("fetcher", {}).get("retry_interval", 5)
        self.max_retries: int = self.config.get("fetcher", {}).get("max_retries", 5)
        
        #Logging setup
        self.logger: logging.Logger = logging.getLogger("DatabentoFetcher")
        log_level = self.config.get("logging", {}).get("level", "INFO")
        self.logger.setLevel(log_level)
        
        #Live client 
        self.client: Optional[db.Live] = None
    
    async def connect(self, symbols: List[str]) -> None:
        """
        Creates and prepares a Databento Live client.
        
        Args:
            symbols (List[str]): List of symbols to subscribe to.
        """
        try:
            #Map schema name to the appropriate Databento schema
            schema = self.schema_mapping.get(self.schema.upper(), "trades")
            
            #Create a new Live client with reconnect policy
            self.client = db.Live(
                key=self.api_key,
                ts_out=True,  #Include timestamp information
                reconnect_policy="reconnect"  #Enable automatic reconnection
            )
            
            #Subscribe to the specified symbols
            self.client.subscribe(
                dataset=self.dataset,
                schema=schema,
                symbols=symbols,
                stype_in=self.stype_in
            )
            
            self.logger.info(f"Created Databento Live client for {self.dataset}, schema: {schema}")
            self.logger.info(f"Subscribed to symbols: {symbols}")
            
        except Exception as e:
            self.logger.error(f"Failed to create Databento Live client: {str(e)}")
            raise ConnectionError(f"Failed to initialize Databento Live client: {str(e)}")
    
    async def stream_data(
        self, 
        symbols: List[str],
        channels: List[str] = None  #Kept for interface compatibility, not used
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Streams real-time market data for the specified symbols.
        
        Args:
            symbols (List[str]): List of symbols to stream data for.
            channels (List[str], optional): Not used for Databento, kept for interface compatibility.
            
        """
        retry_count = 0
        
        while self.should_reconnect:
            try:
                if not self.client:
                    await self.connect(symbols)
                    retry_count = 0
                
                #Start the client if not already started
                self.client.start()
                
                #Process records as they arrive
                for record in self.client:
                    processed_data = self._process_record(record)
                    if processed_data:
                        yield processed_data
                
            except Exception as e:
                self.logger.error(f"Error in Databento datastream: {str(e)}")
                
                if self.client:
                    try:
                        self.client.terminate()
                    except Exception:
                        pass
                    self.client = None
                
                if retry_count < self.max_retries:
                    retry_count += 1
                    wait_time = self.retry_interval * retry_count
                    self.logger.info(f"Reconnecting in {wait_time} seconds (attempt {retry_count}/{self.max_retries})...")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"Maximum retry attempts ({self.max_retries}) reached")
                    self.should_reconnect = False
                    break
    
    def _process_record(self, record) -> Dict[str, Any]:
        """
        Processes and transforms Databento records to match the required format.
        
        Args:
            record: A Databento record object.
        """
        try:
            record_type = type(record).__name__
            if record_type in ('OhlcvMsg', 'Ohlcv1dMsg', 'Ohlcv1hMsg', 'Ohlcv1mMsg'):
                #Process OHLCV messages // standardize
                processed_data = {
                    'time': pd.Timestamp(record.ts_event, unit='ns'),
                    'symbol': str(record.instrument_id),  
                    'open': float(record.open) / 1000000000,  # Floating point conversions
                    'high': float(record.high) / 1000000000,
                    'low': float(record.low) / 1000000000,
                    'close': float(record.close) / 1000000000,
                    'volume': int(record.volume)
                }
                return processed_data
                
            elif record_type == 'TradeMsg':
                processed_data = {
                    'time': pd.Timestamp(record.ts_event, unit='ns'),
                    'symbol': str(record.instrument_id),
                    'open': float(record.price) / 1000000000,
                    'high': float(record.price) / 1000000000,
                    'low': float(record.price) / 1000000000,
                    'close': float(record.price) / 1000000000,
                    'volume': int(record.size)
                }
                return processed_data
                
            elif record_type == 'SymbolMappingMsg':
                self.logger.info(f"Symbol mapping: {record.instrument_id} -> {record.symbol}")
                
                return None
                
            elif record_type in ('ErrorMsg', 'SystemMsg'):
                #Error hangling
                if record_type == 'ErrorMsg':
                    self.logger.error(f"Databento error: {record.err} (code: {record.code})")
                return None
                
            else:
                self.logger.debug(f"Unhandled record type {record_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error processing record {str(e)}")
            return None
    
    async def close(self) -> None:
        """
        Closes the connection cleanly
        """
        self.should_reconnect = False
        
        if self.client:
            try:
                self.client.stop()
                self.logger.info("Databento Live client stopped")
            except Exception as e:
                self.logger.error(f"Error while closing Databento client {str(e)}")
            finally:
                self.client = None
