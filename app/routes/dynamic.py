from fastapi import APIRouter, HTTPException, Query, Body, Request
from starlette.responses import Response
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
    range_filters: Optional[str] = Query(None, description="JSON string for range filters (e.g., {'column': {'gt': value}})"),
    aggregations: Optional[str] = Query(None, description="JSON string for aggregations (e.g., {'column': 'SUM'})"),
    group_by: Optional[str] = Query(None, description="Comma-separated list of columns for grouping"),
    columns: str = Query("*", description="Comma-separated list of columns to select"),
    distinct: Optional[bool] = Query(None, description="Return only distinct rows"),
    limit: int = Query(100, description="Number of rows to retrieve"),
    offset: int = Query(0, description="Number of rows to skip for pagination"),
    format: str = Query("json", description="Response format: 'json' or 'arrow'")
) -> Any:
    """
    Dynamically queries data from a specified schema and table with advanced SQL features such as aggregations,
    grouping, and range filtering. Supports JSON and Apache Arrow response formats.

    Args:
        schema (str): The name of the schema to query.
        table (str): The name of the table to query.
        filters (Optional[str], optional): JSON string for column-value filters (default: None).
        range_filters (Optional[str], optional): JSON string for range filters (e.g., {'column': {'gt': value}}).
        aggregations (Optional[str], optional): JSON string for aggregations (e.g., {'column': 'SUM'}).
        group_by (Optional[str], optional): Comma-separated list of columns for grouping.
        columns (str, optional): Comma-separated list of columns to retrieve (default: "*").
        limit (int, optional): Number of rows to retrieve (default: 100).
        offset (int, optional): Number of rows to skip for pagination.
        format (str): Desired response format: 'json' or 'arrow'.

    Returns:
        JSONResponse or StreamingResponse: Query results in the specified format.
    """
    logging.info(f"Received request for schema: %s, table: %s", schema, table)
    conn: Optional[connection] = None
    try:
        # Establish a database connection
        conn = get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
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

        # Validate group_by columns
        if group_by:
            group_by_columns = group_by.split(",")
            invalid_group_by_columns = [col for col in group_by_columns if col not in valid_columns]
            if invalid_group_by_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid columns in group_by: {', '.join(invalid_group_by_columns)}",
                )

        # Validate aggregations
        aggregation_clauses: List[str] = []
        if aggregations:
            aggregation_dict = json.loads(aggregations)
            invalid_agg_columns = [col for col in aggregation_dict.keys() if col not in valid_columns]
            if invalid_agg_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid columns in aggregations: {', '.join(invalid_agg_columns)}",
                )
            aggregation_clauses = [f"{func}({col}) AS {col}" for col, func in aggregation_dict.items()]
                

        # Validate range filters
        if range_filters:
            range_filter_dict = json.loads(range_filters)
            invalid_range_columns = [col for col in range_filter_dict.keys() if col not in valid_columns]
            if invalid_range_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid columns in range_filters: {', '.join(invalid_range_columns)}",
                )
            
            # Map operators to SQL symbols
            operator_map = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}

            # Generate range clauses using the mapped operators
            range_clauses = [
            f"{col} {operator_map[op]} %s"
            for col, conditions in range_filter_dict.items()
            for op in conditions.keys()
            if op in operator_map
            ]
            range_params = [
                value for conditions in range_filter_dict.values() for value in conditions.values()
            ]
        else:
            range_clauses = []
            range_params = []

        # Build the SQL query dynamically
        select_clause_agg = ", ".join(aggregation_clauses) if aggregation_clauses else columns
        select_cause_dis = "DISTINCT " + columns if distinct else columns
        if aggregation_clauses:
            query = f"SELECT {select_clause_agg} FROM {schema}.{table}"
        elif select_cause_dis:
            query = f"SELECT {select_cause_dis} FROM {schema}.{table}"
        else:
            query = f"SELECT {columns} FROM {schema}.{table}"

        params: List[Any] = []

        # Add filtering conditions
        if filters:
            filter_dict = json.loads(filters)
            filter_clauses = [f"{col} = %s" for col in filter_dict.keys()]
            query += f" WHERE {' AND '.join(filter_clauses)}"
            params.extend(filter_dict.values())

        # Add range filters
        if range_clauses:
            where_or_and = "WHERE" if "WHERE" not in query else "AND"
            query += f" {where_or_and} {' AND '.join(range_clauses)}"
            params.extend(range_params)

        # Add group_by
        if group_by:
            query += f" GROUP BY {group_by}"

        # Determine primary and secondary sorting columns
        primary_sort_column: str = "symbol" if "symbol" in valid_columns else valid_columns[0]

        if primary_sort_column != "time" and "time" in valid_columns:
            secondary_sort_column: Optional[str] = "time"
        elif primary_sort_column != "ts_event" and "ts_event" in valid_columns:
            secondary_sort_column: Optional[str] = "ts_event"
        else:
            secondary_sort_column: Optional[str] = None

        # Add sorting and pagination
        order_by_clause: str = f"{primary_sort_column}, {secondary_sort_column}" if secondary_sort_column else primary_sort_column

        # Omit order by clause if distinct or aggregated (can't use ORDER BY with DISTINCT)
        if distinct or aggregations:
            query += f" LIMIT %s OFFSET %s"
        else:
            query += f" ORDER BY {order_by_clause} LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        # Log the query and parameters
        logging.info("Query: %s", query)

        # Execute the query
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

        # Convert results to a DataFrame
        df: pd.DataFrame = pd.DataFrame(rows, columns=column_names)
        logging.info("Data fetched and converted to DataFrame")

        # Convert timestamps to strings for JSON serialization
        if "time" in df.columns:
            df["time"] = df["time"].astype(str)

        # Format response
        if format == "json":
            return JSONResponse(content=df.to_dict(orient="records"))

        elif format == "arrow":
            # Convert to Arrow Table
            table = pa.Table.from_pandas(df)
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
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn:
            conn.close()

@router.post("/data/{schema}/{table}")
async def insert_data(
    schema: str,
    table: str,
    format: str = Query("json", description="Input data format: 'json' or 'arrow'"),
    request: Request = None,
    payload: Optional[List[Dict[str, Any]]] = Body(None),
) -> Dict[str, Any]:
    """
    Inserts rows of data into the specified schema and table.

    Args:
        schema (str): The schema containing the target table.
        table (str): The table to insert data into.
        format (str, optional): The format of the input data: 'json' or 'arrow' (default: 'json').
        payload (Optional[List[Dict[str, Any]]], optional): A list of JSON objects to insert (default: None).
        request (Request, optional): The incoming request object (default: None).

    Returns:
        Dict[str, Any]: A response indicating the success or failure of the operation.

    Raises:
        HTTPException: If validation fails or the insertion encounters an error.
    """
    logging.info(f"Received request to insert data into schema: %s, table: %s", schema, table)
    conn: Optional[connection] = None
    try:
        # Establish a database connection
        conn = get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Validate schema and table existence
        valid_columns = get_valid_columns(schema, table)

        # Validate input format
        if format not in ["json", "arrow"]:
            raise HTTPException(status_code=400, detail=f"Invalid input format: {format}. Use 'json' or 'arrow'.")
        
        if format == "json":
            if not payload:
                raise HTTPException(status_code=400, detail="Payload is required for JSON data.")
            if not isinstance(payload, list):
                raise HTTPException(status_code=400, detail=f"Invalid payload format: {type(payload)}. Payload must be a list of JSON objects.")
            
            for row in payload:
                if not isinstance(row, dict):
                    raise HTTPException(status_code=400, detail=f"Invalid row format: {type(row)}. Each row must be a JSON object.")
                for col in row.keys():
                    if col not in valid_columns:
                        raise HTTPException(status_code=400, detail=f"Invalid column: {col}")
                for key, value in row.items():
                    if not isinstance(key, str):
                        raise HTTPException(status_code=400, detail=f"Invalid key type: {type(key)}. Keys must be strings.")
                    if isinstance(value, (dict, list)):
                        raise HTTPException(status_code=400, detail=f"Invalid value type for key '{key}': {type(value)}. Values must be scalars.")
            
            # Insert JSON data into the table
            with conn.cursor() as cursor:
                columns = ", ".join(payload[0].keys())
                placeholders = ", ".join(["%s"] * len(payload[0]))
                query = f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})"
                # Log the query and parameters for debugging
                logging.info(f"Query: {query}")
                logging.info(f"Parameters: {payload}")
                cursor.executemany(query, [list(row.values()) for row in payload])
            conn.commit()
            return {"status": "success", "message": f"{len(payload)} row(s) inserted successfully."}

        elif format == "arrow":
            # Ensure the request object exists
            if not request:
                raise HTTPException(status_code=400, detail="Request object is required for Arrow data.")

            try:
                # Validate content type
                content_type = request.headers.get("content-type", "").lower()
                if "application/octet-stream" not in content_type:
                    raise HTTPException(
                        status_code=400,
                        detail="Arrow data must be sent with 'content-type: application/octet-stream'."
                    )

                # Read the raw body as binary
                body = await request.body()

                # Attempt to parse the Arrow table
                try:
                    # Try to open the Arrow stream
                    reader = pa.ipc.open_stream(pa.py_buffer(body))
                    table_data = reader.read_all()
                except pa.lib.ArrowInvalid as stream_error:
                    logging.warning(f"Failed to read Arrow stream: {stream_error}")
                    try:
                        # Fall back to opening as an Arrow file
                        reader = pa.ipc.open_file(pa.py_buffer(body))
                        table_data = reader.read_all()
                    except pa.lib.ArrowInvalid as file_error:
                        logging.error(f"Failed to parse Arrow data: {file_error}")
                        raise HTTPException(
                            status_code=400,
                            detail="Invalid Arrow format: Unable to parse as stream or file."
                        )

                # Log table schema and size
                logging.info(f"Arrow table schema: {table_data.schema}")
                logging.info(f"Arrow table rows: {table_data.num_rows}, columns: {table_data.num_columns}")

                # Validate that all columns exist in the database schema
                for col in table_data.column_names:
                    if col not in valid_columns:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid column in Arrow data: {col}."
                        )

                # Convert Arrow table to rows
                rows = [
                    tuple(row)
                    for row in zip(*[col.to_pylist() for col in table_data.columns])
                ]

                # Insert the rows into the database
                with conn.cursor() as cursor:
                    columns = ", ".join(table_data.column_names)
                    placeholders = ", ".join(["%s"] * len(table_data.column_names))
                    query = f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})"
                    
                    logging.info(f"Executing query: {query}")
                    cursor.executemany(query, rows)

                conn.commit()

                # Return success response
                return {
                    "status": "success",
                    "message": f"Inserted {len(rows)} rows successfully.",
                    "rows_affected": len(rows)
                }

            except pa.lib.ArrowInvalid as e:
                logging.error(f"Arrow parsing error: {e}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid Arrow format: {str(e)}"
                )
            except Exception as e:
                logging.error(f"Unexpected error while inserting Arrow data: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Error processing Arrow data: {str(e)}"
                )
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error inserting data: {e}")
        raise HTTPException(status_code=500, detail=f"Error inserting data: {e}")

    finally:
        if conn:
            conn.close()

@router.put("/data/{schema}/{table}")
async def update_data(
    schema: str,
    table: str,
    filters: Dict[str, Any] = Body(..., description="Conditions to identify rows to update."),
    updates: Dict[str, Any] = Body(..., description="Columns and their new values.")
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

        return {
            "status": "success",
            "message": f"{row_count} row(s) updated successfully.",
            "rows_affected": row_count
        }

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error updating data: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating data: {str(e)}")

    finally:
        if conn:
            conn.close()


@router.delete("/data/{schema}/{table}")
async def delete_data(
    schema: str,
    table: str,
    payload: Dict[str, Any] = Body(..., description="Payload containing filters for deletion.")
) -> Dict[str, Any]:
    """
    Deletes rows of data from the specified schema and table based on filtering conditions.

    Args:
        schema (str): The schema containing the target table.
        table (str): The table to delete data from.
        payload (Dict[str, Any]): A dictionary containing filters to identify rows to delete.

    Returns:
        Dict[str, Any]: A response indicating the success or failure of the operation.

    Raises:
        HTTPException: If validation fails or the deletion encounters an error.
    """
    conn: Optional[connection] = None
    try:
        # Extract filters from the payload
        filters = payload.get("filters")
        if not filters or not isinstance(filters, dict):
            raise HTTPException(status_code=400, detail="Filters must be provided as a dictionary.")

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

        # Combine filter values
        params = list(filters.values())

        # Connect to the database and execute the query
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            row_count = cursor.rowcount  # Get the number of rows affected
        conn.commit()

        return {
            "status": "success",
            "message": f"{row_count} row(s) deleted successfully.",
            "rows_affected": row_count
        }

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error deleting data: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting data: {str(e)}")

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
