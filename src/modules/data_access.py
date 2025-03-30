from typing import List, Dict, Optional, Any, Type, Tuple
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from src.modules.db_models import get_engine, OHLCV
import pandas as pd
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

            # Order by symbol and time
            query = query.order_by(OHLCV.symbol, OHLCV.time)

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
        
    def get_earliest_date(self) -> Optional[str]:
        """
        Retrieves the earliest date from the OHLCV table.

        Returns:
            Optional[str]: The earliest available date in 'YYYY-MM-DD' format, or None if the table is empty.
        """
        with self.Session() as session:
            try:
                earliest_date: Optional[Tuple[Optional[str]]] = (
                    session.query(OHLCV.time)
                    .order_by(OHLCV.time.asc())
                    .first()
                )
                if earliest_date and earliest_date[0]:
                    self.logger.info(f"Earliest available date in the database: {earliest_date[0]}")
                    return earliest_date[0].strftime("%Y-%m-%d")
                return None
            except SQLAlchemyError as e:
                self.logger.error(f"Error retrieving earliest date: {e}")
    
    def get_latest_date(self) -> Optional[str]:
        """
        Retrieves the most recent date from the OHLCV table.

        Returns:
            Optional[str]: The latest available date in 'YYYY-MM-DD' format, or None if the table is empty.
        """
        with self.Session() as session:
            try:
                latest_date: Optional[Tuple[Optional[str]]] = (
                    session.query(OHLCV.time)
                    .order_by(OHLCV.time.desc())
                    .first()
                )
                if latest_date and latest_date[0]:
                    self.logger.info(f"Latest available date in the database: {latest_date[0]}")
                    return latest_date[0].strftime("%Y-%m-%d")
                return None
            except SQLAlchemyError as e:
                self.logger.error(f"Error retrieving latest date: {e}")
                raise

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
                .order_by(OHLCV.time)
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


if __name__ == "__main__":
    data = DataAccess()

    ohlcv_df = pd.DataFrame(data.get_ohlcv_data('2010-06-07', '2024-12-19'), 
                            columns=['time', 'open', 'high', 'low', 'close', 'volume', 'symbol'])
    ohclv_6B_df = pd.DataFrame(data.get_ohlcv_data('2010-06-07', '2024-12-19', ['6B.c.0']), 
                                columns=['time', 'open', 'high', 'low', 'close', 'volume', 'symbol'])

    print(f"Symbols:\n {list(data.get_symbols())}\n")
    print(f"Earliest Date: {data.get_earliest_date()}\n")
    print(f"Latest Date: {data.get_latest_date()}\n")
    print(f"OHLCV: \n{ohlcv_df}\n")
    print(f"OHLCV for 6B: \n{ohclv_6B_df}\n")
    

    

