from typing import Any
import httpx
import pymysql
import json
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("weather")


# Create a MySQL connection
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    database=''
)


# Function to fetch all table names
async def fetch_all_tables()->str:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        cursor.close()  # Good practice to close cursor

        table_names = [table[0] for table in tables]  # Extract names from tuple
        return json.dumps(table_names)

@mcp.tool()
async def get_table_list() -> str:
        """Get the list of all tables present in database
        """
        data=await fetch_all_tables()
        return data



if __name__ == "__main__":
    # Initialize and run the server
    print("Starting sql_mcp server...")
    mcp.run(transport='stdio')




