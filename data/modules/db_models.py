from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Base class for SQLAlchemy models
Base = declarative_base()


class OHLCV(Base):
    """
    SQLAlchemy model representing the `ohlcv_1d` table in the `futures_data` schema.
    
    Attributes:
        time (datetime): The timestamp for the data entry (primary key).
        symbol (str): The symbol or identifier for the instrument (primary key).
        open (float): The opening price for the interval.
        high (float): The highest price for the interval.
        low (float): The lowest price for the interval.
        close (float): The closing price for the interval.
        volume (int): The trading volume for the interval.
    """
    __tablename__ = "ohlcv_1d"
    __table_args__ = {"schema": "futures_data"}

    time: Column = Column(DateTime, primary_key=True, nullable=False)
    symbol: Column = Column(String, primary_key=True, nullable=False)
    open: Column = Column(Float, nullable=False)
    high: Column = Column(Float, nullable=False)
    low: Column = Column(Float, nullable=False)
    close: Column = Column(Float, nullable=False)
    volume: Column = Column(Integer, nullable=False)


def get_engine() -> Engine:
    """
    Create and configure a SQLAlchemy Engine to connect to the TimescaleDB database.
    Database credentials are loaded from a `.env` file.

    Environment Variables:
        - DB_USER (str): The username for database authentication.
        - DB_PASSWORD (str): The password for database authentication.
        - DB_HOST (str): The database server hostname or IP.
        - DB_PORT (str): The port number for database access.
        - DB_NAME (str): The name of the database.

    Returns:
        Engine: A SQLAlchemy Engine object for database interactions.

    Raises:
        ValueError: If any required environment variable is missing.
    """
    # Retrieve database connection parameters from environment variables
    db_user: Optional[str] = os.getenv("DB_USER")
    db_password: Optional[str] = os.getenv("DB_PASSWORD")
    db_host: Optional[str] = os.getenv("DB_HOST")
    db_port: Optional[str] = os.getenv("DB_PORT")
    db_name: Optional[str] = os.getenv("DB_NAME")

    # Validate that all required parameters are present
    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError(
            "One or more required environment variables are missing. "
            "Ensure DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, and DB_NAME are set in the .env file."
        )

    # Build the connection string
    connection_string: str = (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    # Create and return the SQLAlchemy Engine
    return create_engine(connection_string)


def get_session(engine: Engine) -> Session:
    """
    Create a new SQLAlchemy session for database interactions.

    Args:
        engine (Engine): A SQLAlchemy Engine object connected to the database.

    Returns:
        Session: A SQLAlchemy Session object for executing database queries.
    """
    SessionFactory = sessionmaker(bind=engine)
    return SessionFactory()
