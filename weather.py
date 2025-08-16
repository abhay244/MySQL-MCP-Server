from typing import Any
import httpx
from mcp.server.fastmcp import FastMCP
import pymysql
import json
import os
from datetime import date, datetime
# Initialize FastMCP server
mcp = FastMCP("weather")

# Constants
NWS_API_BASE = "https://city.imd.gov.in/"
USER_AGENT = "weather-app/1.0"


async def make_nws_request(url: str) -> dict[str, Any] | None:
    """Make a request to the NWS API with proper error handling."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/geo+json"
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception:
            return None

def format_alert(feature: dict) -> str:
    """Format an alert feature into a readable string."""
    props = feature["properties"]
    return f"""
Event: {props.get('event', 'Unknown')}
Area: {props.get('areaDesc', 'Unknown')}
Severity: {props.get('severity', 'Unknown')}
Description: {props.get('description', 'No description available')}
Instructions: {props.get('instruction', 'No specific instructions provided')}
"""

@mcp.tool()
async def get_alerts(state: str) -> str:
    """Get weather alerts for a Indian city.

    Args:
        city: city name example (e.g. Pune, Delhi)
    """
    url = f"{NWS_API_BASE}/alerts/active/area/{state}"
    data = await make_nws_request(url)

    if not data or "features" not in data:
        return "Unable to fetch alerts or no alerts found."

    if not data["features"]:
        return "No active alerts for this state."

    alerts = [format_alert(feature) for feature in data["features"]]
    return "\n---\n".join(alerts)

@mcp.tool()
async def get_forecast(latitude: float, longitude: float) -> str:
    """Get weather forecast for a location.

    Args:
        latitude: Latitude of the location
        longitude: Longitude of the location
    """
    # First get the forecast grid endpoint
    points_url = f"{NWS_API_BASE}/points/{latitude},{longitude}"
    points_data = await make_nws_request(points_url)

    if not points_data:
        return "Unable to fetch forecast data for this location."

    # Get the forecast URL from the points response
    forecast_url = points_data["properties"]["forecast"]
    forecast_data = await make_nws_request(forecast_url)

    if not forecast_data:
        return "Unable to fetch detailed forecast."

    # Format the periods into a readable forecast
    periods = forecast_data["properties"]["periods"]
    forecasts = []
    for period in periods[:5]:  # Only show next 5 periods
        forecast = f"""
{period['name']}:
Temperature: {period['temperature']}°{period['temperatureUnit']}
Wind: {period['windSpeed']} {period['windDirection']}
Forecast: {period['detailedForecast']}
"""
        forecasts.append(forecast)

    return "\n---\n".join(forecasts)




# Create a MySQL connection
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    database=''
)

# Function to fetch all table names
def convert_dates(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj
async def fetch_all_tables()->str:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        cursor.close()  # Good practice to close cursor

        table_names = [table[0] for table in tables]  # Extract names from tuple
        return json.dumps(table_names)

@mcp.tool()
async def fetch_data(query:str)->str:
        """
        This tool takes sql query as input related to all data fetch request either for new_users table or for m_loan table
        and then executes the query and return all data related to that query
        :return: all the fetched from query
        """
        cursor = connection.cursor()
        cursor.execute(query)
        dataList = cursor.fetchall()
        cursor.close()

        queryData = [tuple(convert_dates(col) for col in row) for row in dataList] # or use dict if you know column names
        return json.dumps(queryData)

@mcp.tool()
async def get_table_list() -> str:
        """Get the list of all tables present in database
        """
        data=await fetch_all_tables()
        return data


# @mcp.tool()
# async def get_user_info(user_id: int) -> str:
#         """Get the a particular user from id
#         """
#         data=await getUserFromId(user_id)
#         return data


Intro_file=os.path.join(os.path.dirname(__file__),"intro.txt")
print(f"file path is: {Intro_file}")

@mcp.resource("intro://intro")
async def get_application_intro()->str:
    """
    When the server starts read the resource and before giving answers consider these points also,
    this text file also contains table information and structure, when creating sql query takes columns name from here
    """
    with open(Intro_file,"r") as f:
        content=f.read().strip()
    return content or "NO intro found"







if __name__ == "__main__":
    # Initialize and run the server
    print("Starting weather server...")
    mcp.run(transport='stdio')