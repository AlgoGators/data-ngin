from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from app.db import get_connection
from typing import Optional, List, Dict, Any
from psycopg2.extensions import connection
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import io
import json

# Create an APIRouter instance to handle routes for exporting data
router: APIRouter = APIRouter()

@router.get("/export/{schema}/{table}")
async def export_data(
    schema: str,
    table: str,
    filters: Optional[str] = Query(None, description="JSON string for column-value filters"),
    columns: str = Query("*", description="Comma-separated list of columns to select"),
    limit: int = Query(100, description="Number of rows to retrieve"),
    offset: int = Query(0, description="Number of rows to skip for pagination"),
    format: str = Query("json", description="Format to export the data in: 'json', 'arrow', or 'csv'"),
) -> StreamingResponse:
    """
    Exports data from the specified schema and table in the requested format.

    Args:
        schema (str): The name of the schema to query.
        table (str): The name of the table to query.
        filters (Optional[str], optional): JSON string for column-value filters (default: None).
        columns (str, optional): Comma-separated list of columns to retrieve (default: "*").
        limit (int, optional): Number of rows to retrieve (default: 100).
        offset (int, optional): Number of rows to skip for pagination (default: 0).
        format (str): Desired output format ('json', 'arrow', or 'csv').

    Returns:
        StreamingResponse: The data exported in the requested format.

    Raises:
        HTTPException: If the schema or table does not exist, or an invalid format is provided.
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
            try:
                filter_dict: Dict[str, Any] = json.loads(filters)
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Invalid filters JSON: {str(e)}")
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

        # Convert results to a DataFrame
        df: pd.DataFrame = pd.DataFrame(rows, columns=column_names)

        # Export in the requested format
        if format == "json":
            # Convert timestamps to strings for JSON serialization
            if "time" in df.columns:
                df["time"] = df["time"].astype(str)

            # Return data as JSON response
            return JSONResponse(content=df.to_dict(orient="records"))

        elif format == "csv":
            # Convert DataFrame to CSV and return as a stream
            csv_output: io.StringIO = io.StringIO()
            df.to_csv(csv_output, index=False)
            csv_output.seek(0)
            return StreamingResponse(
                iter([csv_output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=data.csv"},
            )

        else:
            # Raise an error if the requested format is invalid
            raise HTTPException(status_code=400, detail="Invalid format. Use 'json', 'arrow', or 'csv'.")

    except Exception as e:
        # Raise an HTTP 500 error for any unexpected issues
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Ensure the database connection is closed after execution
        if conn:
            conn.close()
