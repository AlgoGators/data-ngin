from fastapi import APIRouter, HTTPException
from typing import List, Dict
from psycopg2.extensions import connection
from app.db import get_connection

router: APIRouter = APIRouter()

@router.get("/metadata/schemas")
async def get_schemas() -> List[str]:
    """
    Retrieves all schemas in the database.

    Returns:
        List[str]: A list of schema names.

    Raises:
        HTTPException: If there is an error querying the database.
    """
    conn: connection = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT LIKE '_timescaledb%'  -- Exclude TimescaleDB internal schemas
                AND schema_name NOT LIKE 'timescaledb_%'
                AND schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')  -- Exclude system schemas
                """
            )
            schemas: List[str] = [row[0] for row in cursor.fetchall()]
        return schemas
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving schemas: {e}")
    finally:
        if conn:
            conn.close()

@router.get("/metadata/tables/{schema}")
async def get_tables(schema: str) -> List[str]:
    """
    Retrieves all tables in a specified schema.

    Args:
        schema (str): The name of the schema.

    Returns:
        List[str]: A list of table names.

    Raises:
        HTTPException: If the schema does not exist or there is an error querying the database.
    """
    conn: connection = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                """,
                (schema,),
            )
            tables: List[str] = [row[0] for row in cursor.fetchall()]
            if not tables:
                raise HTTPException(status_code=404, detail="Schema not found or has no tables.")
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving tables: {e}")
    finally:
        if conn:
            conn.close()

@router.get("/metadata/columns/{schema}/{table}")
async def get_columns(schema: str, table: str) -> List[Dict[str, str]]:
    """
    Retrieves all columns for a specified table within a schema.

    Args:
        schema (str): The name of the schema.
        table (str): The name of the table.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing column names and their data types.

    Raises:
        HTTPException: If the table does not exist or there is an error querying the database.
    """
    conn: connection = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            columns: List[Dict[str, str]] = [
                {"column_name": row[0], "data_type": row[1]} for row in cursor.fetchall()
            ]
            if not columns:
                raise HTTPException(status_code=404, detail="Table not found or has no columns.")
        return columns
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving columns: {e}")
    finally:
        if conn:
            conn.close()
