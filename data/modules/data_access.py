from typing import List, Dict, Optional, Any, Type, Tuple
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from data.modules.db_models import get_engine, OHLCV
import logging


class DataAccess:
    """
    A data access layer for querying the OHLCV table in PostgreSQL using SQLAlchemy ORM.
    """

    def __init__(self) -> None:
        """
        Initializes the DataAccess class by creating a database engine and session maker.
        """
        self.engine: Engine = get_engine()
        self.Session: Type[sessionmaker] = sessionmaker(bind=self.engine)
        self.logger: logging.Logger = logging.getLogger("DataAccess")
        self.logger.setLevel(logging.INFO)

    def get_ohlcv_data(
        self, 
        start_date: str, 
        end_date: str, 
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves OHLCV data for the specified date range and symbols.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.
            symbols (Optional[List[str]]): A list of symbols to filter.

        Returns:
            List[Dict[str, Any]]: A list of OHLCV records.
        """
        with self.Session() as session:
            query = session.query(OHLCV).filter(
                OHLCV.time.between(start_date, end_date)
            )
            if symbols:
                query = query.filter(OHLCV.symbol.in_(symbols))

            data: List[OHLCV] = query.all()
            result: List[Dict[str, Any]] = [record.__dict__ for record in data if record]
            
            # Remove SQLAlchemy's internal state metadata
            for record in result:
                record.pop("_sa_instance_state", None)
            return result

    def get_symbols(self) -> List[str]:
        """
        Retrieves all unique symbols from the OHLCV table.

        Returns:
            List[str]: A list of unique symbols.
        """
        with self.Session() as session:  
            symbols: List[Tuple[str]] = session.query(OHLCV.symbol).distinct().all()
            return [symbol[0] for symbol in symbols]

    def get_latest_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the most recent OHLCV record for a given symbol.

        Args:
            symbol (str): The symbol to retrieve the latest data for.

        Returns:
            Optional[Dict[str, Any]]: The latest OHLCV record or None if not found.
        """
        with self.Session() as session:  
            latest_data: Optional[OHLCV] = (
                session.query(OHLCV)
                .filter_by(symbol=symbol)
                .order_by(OHLCV.time.desc())
                .first()
            )
            if latest_data:
                result: Dict[str, Any] = latest_data.__dict__
                result.pop("_sa_instance_state", None)
                return result
            return None

    def insert_data(self, records: List[Dict[str, Any]]) -> None:
        """
        Inserts new OHLCV records into the database.

        Args:
            records (List[Dict[str, Any]]): A list of OHLCV records to insert.

        Raises:
            SQLAlchemyError: If an error occurs during the insertion process.
        """
        with self.Session() as session:  
            try:
                objects: List[OHLCV] = [OHLCV(**record) for record in records]
                session.add_all(objects)
                session.commit()
                self.logger.info(f"Inserted {len(objects)} records successfully.")
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"Error inserting data: {e}")
                raise

    def delete_data(
        self, 
        start_date: str, 
        end_date: str, 
        symbols: Optional[List[str]] = None
    ) -> None:
        """
        Deletes OHLCV records within a specified date range and symbols.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.
            symbols (Optional[List[str]]): A list of symbols to filter.

        Raises:
            SQLAlchemyError: If an error occurs during the deletion process.
        """
        with self.Session() as session:  
            try:
                query = session.query(OHLCV).filter(
                    OHLCV.time.between(start_date, end_date)
                )
                if symbols:
                    query = query.filter(OHLCV.symbol.in_(symbols))
                
                rows_deleted: int = query.delete(synchronize_session=False)
                session.commit()
                self.logger.info(f"Deleted {rows_deleted} records successfully.")
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"Error deleting data: {e}")
                raise
