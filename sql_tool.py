from typing import Any, List, Dict
import httpx
import pymysql
import json
import re
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("weather")


# Create a MySQL connection
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Abhay@#1001',
    database='testdb',
    cursorclass=pymysql.cursors.DictCursor  # Use dict cursor for better data handling
)


# Function to fetch all table names
async def fetch_all_tables()->str:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        cursor.close()  # Good practice to close cursor

        table_names = [table[0] for table in tables]  # Extract names from tuple
        return json.dumps(table_names)

# Function to get table schema
async def fetch_table_schema(table_name: str) -> Dict:
    cursor = connection.cursor()
    try:
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        
        schema = {
            "table_name": table_name,
            "columns": []
        }
        
        for column in columns:
            column_info = {
                "name": column["Field"],
                "type": column["Type"],
                "null": column["Null"] == "YES",
                "key": column["Key"],
                "default": column["Default"],
                "extra": column["Extra"]
            }
            schema["columns"].append(column_info)
        
        return schema
    finally:
        cursor.close()

# Function to execute safe queries
async def execute_safe_query(query: str, params: tuple = None) -> Dict:
    cursor = connection.cursor()
    try:
        # Check if query is safe (only SELECT statements)
        query_type = query.strip().upper().split()[0]
        if query_type not in ['SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN']:
            return {
                "error": "Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed for security reasons",
                "success": False
            }
        
        cursor.execute(query, params)
        
        if query_type in ['SELECT']:
            results = cursor.fetchall()
            return {
                "success": True,
                "data": results,
                "row_count": len(results)
            }
        else:
            results = cursor.fetchall()
            return {
                "success": True,
                "data": results,
                "row_count": len(results)
            }
            
    except Exception as e:
        return {
            "error": str(e),
            "success": False
        }
    finally:
        cursor.close()

# Function to generate SQL query based on user context
def generate_sql_query(user_request: str, table_schemas: Dict) -> str:
    """
    Generate SQL query based on user request and available table schemas
    This is a basic implementation - can be enhanced with AI/LLM integration
    """
    user_request_lower = user_request.lower()
    
    # Simple keyword-based query generation
    if "count" in user_request_lower:
        # Find table name in the request
        for table_name in table_schemas:
            if table_name.lower() in user_request_lower:
                return f"SELECT COUNT(*) as total_count FROM {table_name}"
    
    elif "all" in user_request_lower or "select" in user_request_lower:
        for table_name in table_schemas:
            if table_name.lower() in user_request_lower:
                return f"SELECT * FROM {table_name} LIMIT 10"
    
    elif "average" in user_request_lower or "avg" in user_request_lower:
        # Try to find numeric columns and table
        for table_name, schema in table_schemas.items():
            if table_name.lower() in user_request_lower:
                numeric_columns = [col["name"] for col in schema["columns"] 
                                 if any(t in col["type"].lower() for t in ["int", "decimal", "float", "double"])]
                if numeric_columns:
                    return f"SELECT AVG({numeric_columns[0]}) as average_{numeric_columns[0]} FROM {table_name}"
    
    elif "max" in user_request_lower or "maximum" in user_request_lower:
        for table_name, schema in table_schemas.items():
            if table_name.lower() in user_request_lower:
                numeric_columns = [col["name"] for col in schema["columns"] 
                                 if any(t in col["type"].lower() for t in ["int", "decimal", "float", "double", "date", "time"])]
                if numeric_columns:
                    return f"SELECT MAX({numeric_columns[0]}) as max_{numeric_columns[0]} FROM {table_name}"
    
    elif "min" in user_request_lower or "minimum" in user_request_lower:
        for table_name, schema in table_schemas.items():
            if table_name.lower() in user_request_lower:
                numeric_columns = [col["name"] for col in schema["columns"] 
                                 if any(t in col["type"].lower() for t in ["int", "decimal", "float", "double", "date", "time"])]
                if numeric_columns:
                    return f"SELECT MIN({numeric_columns[0]}) as min_{numeric_columns[0]} FROM {table_name}"
    
    elif "group by" in user_request_lower or "grouped by" in user_request_lower:
        for table_name, schema in table_schemas.items():
            if table_name.lower() in user_request_lower:
                text_columns = [col["name"] for col in schema["columns"] 
                              if any(t in col["type"].lower() for t in ["varchar", "char", "text", "enum"])]
                if text_columns:
                    return f"SELECT {text_columns[0]}, COUNT(*) as count FROM {table_name} GROUP BY {text_columns[0]}"
    
    return "-- Unable to generate query automatically. Please provide more specific requirements."

def build_advanced_query(table_name: str, columns: List[str] = None, where_conditions: Dict = None, 
                        order_by: str = None, limit: int = None, group_by: str = None) -> str:
    """
    Build a more advanced SQL query with various options
    """
    # Build SELECT clause
    if columns:
        select_clause = f"SELECT {', '.join(columns)}"
    else:
        select_clause = "SELECT *"
    
    # Build FROM clause
    from_clause = f"FROM {table_name}"
    
    # Build WHERE clause
    where_clause = ""
    if where_conditions:
        conditions = []
        for column, condition in where_conditions.items():
            if isinstance(condition, dict):
                operator = condition.get('operator', '=')
                value = condition.get('value')
                if isinstance(value, str):
                    conditions.append(f"{column} {operator} '{value}'")
                else:
                    conditions.append(f"{column} {operator} {value}")
            else:
                conditions.append(f"{column} = '{condition}'")
        
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"
    
    # Build GROUP BY clause
    group_clause = f"GROUP BY {group_by}" if group_by else ""
    
    # Build ORDER BY clause
    order_clause = f"ORDER BY {order_by}" if order_by else ""
    
    # Build LIMIT clause
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Combine all clauses
    query_parts = [select_clause, from_clause, where_clause, group_clause, order_clause, limit_clause]
    query = " ".join([part for part in query_parts if part])
    
    return query

@mcp.tool()
async def get_table_list() -> str:
        """Get the list of all tables present in database
        """
        data=await fetch_all_tables()
        return data

@mcp.tool()
async def get_table_schema(table_name: str) -> str:
    """Get the schema/structure of a specific table including column names, types, and constraints
    
    Args:
        table_name: Name of the table to get schema for
    """
    try:
        schema = await fetch_table_schema(table_name)
        return json.dumps(schema, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to get schema for table '{table_name}': {str(e)}"})

@mcp.tool()
async def get_all_table_schemas() -> str:
    """Get the schema for all tables in the database
    """
    try:
        tables_json = await fetch_all_tables()
        table_names = json.loads(tables_json)
        
        all_schemas = {}
        for table_name in table_names:
            schema = await fetch_table_schema(table_name)
            all_schemas[table_name] = schema
        
        return json.dumps(all_schemas, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to get all schemas: {str(e)}"})

@mcp.tool()
async def create_sql_query(user_request: str, include_schemas: bool = True) -> str:
    """Generate SQL query based on user request and table schemas
    
    Args:
        user_request: Description of what data the user wants to query
        include_schemas: Whether to include table schemas in the response for context
    """
    try:
        # Get all table schemas for context
        tables_json = await fetch_all_tables()
        table_names = json.loads(tables_json)
        
        table_schemas = {}
        for table_name in table_names:
            schema = await fetch_table_schema(table_name)
            table_schemas[table_name] = schema
        
        # Generate SQL query
        generated_query = generate_sql_query(user_request, table_schemas)
        
        result = {
            "user_request": user_request,
            "generated_query": generated_query,
            "note": "Review the query before executing. Modify as needed for your specific requirements."
        }
        
        if include_schemas:
            result["available_tables"] = table_schemas
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to create SQL query: {str(e)}"})

@mcp.tool()
async def execute_query(sql_query: str) -> str:
    """Execute a SQL query safely (only SELECT, SHOW, DESCRIBE, EXPLAIN queries allowed)
    
    Args:
        sql_query: The SQL query to execute
    """
    try:
        result = await execute_safe_query(sql_query)
        return json.dumps(result, indent=2, default=str)  # default=str handles datetime objects
    except Exception as e:
        return json.dumps({"error": f"Failed to execute query: {str(e)}", "success": False})

@mcp.tool()
async def get_sample_data(table_name: str, limit: int = 5) -> str:
    """Get sample data from a specific table
    
    Args:
        table_name: Name of the table to get sample data from
        limit: Number of rows to return (default: 5, max: 100)
    """
    try:
        # Limit the maximum number of rows for safety
        limit = min(limit, 100)
        
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        result = await execute_safe_query(query)
        
        if result["success"]:
            return json.dumps({
                "table_name": table_name,
                "sample_data": result["data"],
                "row_count": result["row_count"],
                "note": f"Showing first {limit} rows"
            }, indent=2, default=str)
        else:
            return json.dumps(result, indent=2)
            
    except Exception as e:
        return json.dumps({"error": f"Failed to get sample data: {str(e)}", "success": False})


@mcp.tool()
async def build_custom_query(table_name: str, columns: str = None, where_conditions: str = None, 
                           order_by: str = None, limit: int = None, group_by: str = None) -> str:
    """Build a custom SQL query with specified parameters
    
    Args:
        table_name: Name of the table to query
        columns: Comma-separated list of columns (default: all columns)
        where_conditions: WHERE conditions in JSON format, e.g. '{"age": {"operator": ">", "value": 25}}'
        order_by: Column name to order by
        limit: Maximum number of rows to return
        group_by: Column name to group by
    """
    try:
        # Parse columns
        column_list = None
        if columns:
            column_list = [col.strip() for col in columns.split(',')]
        
        # Parse where conditions
        where_dict = None
        if where_conditions:
            try:
                where_dict = json.loads(where_conditions)
            except json.JSONDecodeError:
                return json.dumps({"error": "Invalid JSON format for where_conditions"})
        
        # Build the query
        query = build_advanced_query(
            table_name=table_name,
            columns=column_list,
            where_conditions=where_dict,
            order_by=order_by,
            limit=limit,
            group_by=group_by
        )
        
        result = {
            "generated_query": query,
            "parameters": {
                "table_name": table_name,
                "columns": column_list,
                "where_conditions": where_dict,
                "order_by": order_by,
                "limit": limit,
                "group_by": group_by
            },
            "note": "Review the generated query before executing"
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to build custom query: {str(e)}"})

@mcp.tool()
async def analyze_table_relationships() -> str:
    """Analyze foreign key relationships between tables
    """
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT 
                TABLE_NAME,
                COLUMN_NAME,
                CONSTRAINT_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                REFERENCED_TABLE_NAME IS NOT NULL
                AND TABLE_SCHEMA = DATABASE()
        """)
        
        relationships = cursor.fetchall()
        cursor.close()
        
        return json.dumps({
            "foreign_keys": relationships,
            "relationship_count": len(relationships),
            "note": "Foreign key relationships help understand how tables are connected"
        }, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to analyze relationships: {str(e)}"})

@mcp.tool()
async def suggest_query_improvements(sql_query: str) -> str:
    """Analyze a SQL query and suggest improvements
    
    Args:
        sql_query: The SQL query to analyze
    """
    try:
        suggestions = []
        query_upper = sql_query.upper().strip()
        
        # Basic query analysis
        if "SELECT *" in query_upper:
            suggestions.append("Consider selecting specific columns instead of SELECT * for better performance")
        
        if "LIMIT" not in query_upper and "SELECT" in query_upper:
            suggestions.append("Consider adding LIMIT clause to prevent accidentally fetching too many rows")
        
        if "WHERE" not in query_upper and "SELECT" in query_upper:
            suggestions.append("Consider adding WHERE clause to filter results if needed")
        
        if "ORDER BY" not in query_upper and "SELECT" in query_upper:
            suggestions.append("Consider adding ORDER BY clause for consistent result ordering")
        
        # Check for potential SQL injection risks
        if any(char in sql_query for char in ["'", '"', ";", "--"]):
            suggestions.append("Be cautious of potential SQL injection - use parameterized queries when possible")
        
        result = {
            "original_query": sql_query,
            "suggestions": suggestions if suggestions else ["Query looks good! No obvious improvements needed."],
            "note": "These are basic suggestions. Always test queries carefully."
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to analyze query: {str(e)}"})


if __name__ == "__main__":
    # Initialize and run the server
    print("Starting sql_mcp server...")
    mcp.run(transport='stdio')




