from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
from psycopg2.extensions import connection
import json
from app.db import get_connection
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import pyarrow as pa
import io
import logging

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
    format: str = Query("json", description="Response format: 'json' or 'arrow'")
) -> List[Dict[str, Any]]:
    """
    Dynamically queries data from a specified schema and table with optional filtering, pagination, column selection,
    and default sorting by time and symbol (if available).

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
        # Fetch valid column names
        valid_columns: List[str] = get_valid_columns(schema, table)

        # Validate user-specified columns
        if columns != "*":
            requested_columns: List[str] = columns.split(",")
            invalid_columns: List[str] = [
                col for col in requested_columns if col not in valid_columns
            ]
            if invalid_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid columns requested: {', '.join(invalid_columns)}",
                )
        
        # Determine primary and secondary sorting columns
        primary_sort_column: str = "symbol" if "symbol" in valid_columns else valid_columns[0]

        if primary_sort_column != "time" and "time" in valid_columns:
            secondary_sort_column: Optional[str] = "time"
        elif primary_sort_column != "ts_event" and "ts_event" in valid_columns:
            secondary_sort_column: Optional[str] = "ts_event"
        else:
            secondary_sort_column: Optional[str] = None
            
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

        # Add sorting and pagination
        order_by_clause: str = (
            f"{primary_sort_column}, {secondary_sort_column}" if secondary_sort_column else primary_sort_column
        )
        query += f" ORDER BY {order_by_clause} LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        # Execute the query and fetch results
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows: List[tuple] = cursor.fetchall()
            column_names: List[str] = [desc[0] for desc in cursor.description]

        # Convert results to a DataFrame
        df: pd.DataFrame = pd.DataFrame(rows, columns=column_names)

        # Format response
        if format == "json":
            # Convert timestamps to strings for JSON serialization
            if "time" in df.columns:
                df["time"] = df["time"].astype(str)

            # Return data as JSON response
            return JSONResponse(content=df.to_dict(orient="records"))

        elif format == "arrow":
            # Convert to Arrow Table
            table = pa.Table.from_pandas(df)

            # Return as binary stream
            stream = pa.BufferOutputStream()
            with pa.ipc.new_file(stream, table.schema) as writer:
                writer.write(table)
            binary = stream.getvalue().to_pybytes()

            return StreamingResponse(
                io.BytesIO(binary),
                media_type="application/octet-stream",
                headers={"Content-Disposition": "inline; filename=data.arrow"},
            )

        else:
            raise HTTPException(status_code=400, detail=f"Invalid format '{format}'. Use 'json' or 'arrow'.")


    except Exception as e:
        # Raise an HTTP 500 error for any unexpected issues during query execution
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Ensure the database connection is closed after execution
        if conn:
            conn.close()

@router.post("/data/{schema}/{table}")
async def insert_data(
    schema: str,
    table: str,
    rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Inserts rows of data into the specified schema and table.

    Args:
        schema (str): The schema containing the target table.
        table (str): The table to insert data into.
        rows (List[Dict[str, Any]]): A list of dictionaries where each dictionary represents a row of data.

    Returns:
        Dict[str, Any]: A response indicating the success or failure of the operation.

    Raises:
        HTTPException: If validation fails or the insertion encounters an error.
    """
    conn: Optional[connection] = None
    try:
        # Validate schema and table existence
        valid_columns = get_valid_columns(schema, table)

        # Validate input data
        for row in rows:
            invalid_columns = [col for col in row.keys() if col not in valid_columns]
            if invalid_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid columns in input: {', '.join(invalid_columns)}",
                )

        # Build the SQL query dynamically
        columns = rows[0].keys()
        placeholders = ", ".join([f"%({col})s" for col in columns])
        query = f"INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES ({placeholders})"

        # Connect to the database and execute the query
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)  # Execute the query for all rows
        conn.commit()

        return {"status": "success", "message": f"{len(rows)} row(s) inserted successfully."}

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error inserting data: {e}")

    finally:
        if conn:
            conn.close()

@router.put("/data/{schema}/{table}")
async def update_data(
    schema: str,
    table: str,
    filters: Dict[str, Any],
    updates: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Updates rows of data in the specified schema and table based on filtering conditions.

    Args:
        schema (str): The schema containing the target table.
        table (str): The table to update data in.
        filters (Dict[str, Any]): A dictionary specifying conditions to identify rows to update.
        updates (Dict[str, Any]): A dictionary specifying columns and their new values.

    Returns:
        Dict[str, Any]: A response indicating the success or failure of the operation.

    Raises:
        HTTPException: If validation fails or the update encounters an error.
    """
    conn: Optional[connection] = None
    try:
        # Validate schema and table existence
        valid_columns = get_valid_columns(schema, table)

        # Validate update columns
        invalid_update_columns = [col for col in updates.keys() if col not in valid_columns]
        if invalid_update_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid columns in updates: {', '.join(invalid_update_columns)}",
            )

        # Validate filter columns
        invalid_filter_columns = [col for col in filters.keys() if col not in valid_columns]
        if invalid_filter_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid columns in filters: {', '.join(invalid_filter_columns)}",
            )

        # Build the SQL query dynamically
        set_clause = ", ".join([f"{col} = %s" for col in updates.keys()])
        where_clause = " AND ".join([f"{col} = %s" for col in filters.keys()])
        query = f"UPDATE {schema}.{table} SET {set_clause} WHERE {where_clause}"

        # Combine values for SET and WHERE clauses
        params = list(updates.values()) + list(filters.values())

        # Connect to the database and execute the query
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            row_count = cursor.rowcount  # Get the number of rows affected
        conn.commit()

        return {"status": "success", "message": f"{row_count} row(s) updated successfully."}

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating data: {e}")

    finally:
        if conn:
            conn.close()

@router.delete("/data/{schema}/{table}")
async def delete_data(
    schema: str,
    table: str,
    filters: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Deletes rows of data from the specified schema and table based on filtering conditions.

    Args:
        schema (str): The schema containing the target table.
        table (str): The table to delete data from.
        filters (Dict[str, Any]): A dictionary specifying conditions to identify rows to delete.

    Returns:
        Dict[str, Any]: A response indicating the success or failure of the operation.

    Raises:
        HTTPException: If validation fails or the deletion encounters an error.
    """
    conn: Optional[connection] = None
    try:
        # Validate schema and table existence
        valid_columns = get_valid_columns(schema, table)

        # Validate filter columns
        invalid_filter_columns = [col for col in filters.keys() if col not in valid_columns]
        if invalid_filter_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid columns in filters: {', '.join(invalid_filter_columns)}",
            )

        # Build the SQL query dynamically
        where_clause = " AND ".join([f"{col} = %s" for col in filters.keys()])
        query = f"DELETE FROM {schema}.{table} WHERE {where_clause}"

        # Combine values for WHERE clause
        params = list(filters.values())

        # Connect to the database and execute the query
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            row_count = cursor.rowcount  # Get the number of rows affected
        conn.commit()

        return {"status": "success", "message": f"{row_count} row(s) deleted successfully."}

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting data: {e}")

    finally:
        if conn:
            conn.close()


def get_valid_columns(schema: str, table: str) -> List[str]:
    """
    Fetches the valid column names for a specified schema and table.

    Args:
        schema (str): The name of the schema.
        table (str): The name of the table.

    Returns:
        List[str]: A list of valid column names.

    Raises:
        HTTPException: If the schema or table does not exist.
    """
    conn: Optional[connection] = None
    try:
        # Connect to the database
        conn = get_connection()
        with conn.cursor() as cursor:
            # Query valid column names from the database
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            columns: List[tuple] = cursor.fetchall()
            if not columns:
                raise HTTPException(status_code=404, detail="Schema or table not found.")
            return [col[0] for col in columns]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching column names: {e}")
    finally:
        if conn:
            conn.close()
