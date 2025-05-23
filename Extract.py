import requests
import json
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def fetch_real_estate_data(api_key: str, limit: int = 500) -> list:
    """
    Fetches random property data from the Realty Mole API.

    Args:
        api_key (str): Your RapidAPI key.
        limit (int): Number of properties to retrieve.

    Returns:
        list: A list of property dictionaries.
    """
    url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
    }
    querystring = {"limit": str(limit)}

    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def save_data_to_json(data: list, filename: str = r'Raw_file\OOreal_estate.json') -> None:
    # Use raw string for Windows file path
    os.makedirs(os.path.dirname(filename), exist_ok=True)  # Make sure directory exists
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

def load_data_to_dataframe(filename: str = r'Raw_file\OOreal_estate.json') -> pd.DataFrame:
    with open(filename) as f:
        data = json.load(f)
    # If data is a list of dictionaries (likely), create DataFrame directly
    if isinstance(data, list):
        return pd.DataFrame(data)
    # If data is a dictionary of scalars
    elif isinstance(data, dict):
        return pd.DataFrame([data])
    else:
        raise ValueError("Unsupported JSON data format")

# Example usage:
# api_key = os.getenv('RAPIDAPI_KEY')
# data = fetch_real_estate_data(api_key)
# save_data_to_json(data)
# real_estate_df = load_data_to_dataframe()
# print(real_estate_df.head())
