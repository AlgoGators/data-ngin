from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
from psycopg2.extensions import connection
import json
from app.db import get_connection

# Create an APIRouter instance to handle routes for dynamic querying
router: APIRouter = APIRouter()

@router.get("/data/{schema}/{table}")
async def get_data(
    schema: str,
    table: str,
    filters: Optional[str] = Query(None, description="JSON string for column-value filters"),
    columns: str = Query("*", description="Comma-separated list of columns to select"),
    limit: int = Query(100, description="Number of rows to retrieve"),
    offset: int = Query(0, description="Number of rows to skip for pagination"),
) -> List[Dict[str, Any]]:
    """
    Dynamically queries data from a specified schema and table with optional filtering, pagination, and column selection.

    Args:
        schema (str): The name of the schema to query.
        table (str): The name of the table to query.
        filters (Optional[str], optional): JSON string with column-value pairs for filtering (default: None).
        columns (str, optional): Comma-separated list of columns to retrieve (default: "*").
        limit (int, optional): Number of rows to retrieve (default: 100).
        offset (int, optional): Number of rows to skip for pagination (default: 0).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing query results.

    Raises:
        HTTPException: If the schema or table does not exist, or if a query error occurs.
    """
    conn: Optional[connection] = None
    try:
        # Establish a database connection
        conn = get_connection()

        # Validate the existence of the schema and table in the database
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            if not cursor.fetchone():
                # Raise an HTTP 404 error if the schema or table does not exist
                raise HTTPException(status_code=404, detail="Schema or table not found.")

        # Build the SQL query
        query: str = f"SELECT {columns} FROM {schema}.{table}"
        params: List[Any] = []

        # Add filtering conditions if provided
        if filters:
            filter_dict: Dict[str, Any] = json.loads(filters)
            filter_clauses: List[str] = [f"{col} = %s" for col in filter_dict.keys()]
            query += f" WHERE {' AND '.join(filter_clauses)}"
            params.extend(filter_dict.values())

        # Add pagination clauses
        query += " ORDER BY 1 LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        # Execute the query and fetch results
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows: List[tuple] = cursor.fetchall()
            column_names: List[str] = [desc[0] for desc in cursor.description]

        # Format query results as a list of dictionaries
        return [dict(zip(column_names, row)) for row in rows]

    except Exception as e:
        # Raise an HTTP 500 error for any unexpected issues during query execution
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Ensure the database connection is closed after execution
        if conn:
            conn.close()
