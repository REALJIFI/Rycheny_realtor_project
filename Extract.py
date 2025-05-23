import requests
import json
import pandas as pd

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

def save_data_to_json(data: list, filename: str = 'real_estate.json') -> None:
    """
    Saves a list of data to a JSON file.

    Args:
        data (list): The data to save.
        filename (str): The name of the file to save to.
    """
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

def load_data_to_dataframe(filename: str = 'real_estate.json') -> pd.DataFrame:
    """
    Loads JSON data from a file into a pandas DataFrame.

    Args:
        filename (str): Path to the JSON file.

    Returns:
        pd.DataFrame: The loaded data as a DataFrame.
    """
    with open(filename, 'r') as file:
        data = json.load(file)
    return pd.DataFrame(data)

# Example usage block
if __name__ == "__main__":
    API_KEY = "e64c48a3b5mshfe9cdd04e4c4a29p18e80bjsn55c3acbad527"
    
    # Fetch and save data
    properties = fetch_real_estate_data(api_key=API_KEY)
    save_data_to_json(properties)

    # Load data into DataFrame and preview
    df = load_data_to_dataframe()
    print(df.head())
