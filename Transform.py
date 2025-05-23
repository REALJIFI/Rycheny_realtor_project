
import pandas as pd

def clean_real_estate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the real estate DataFrame by replacing missing values with defaults.

    Args:
        df (pd.DataFrame): Raw DataFrame with possible missing values.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    df = df.copy()  # avoid modifying input outside
    
    df.fillna({
        'bathrooms': 0.0,
        'bedrooms': 0.0,
        'squareFootage': 0.0,
        'county': 'unknown',
        'propertyType': 'unknown',
        'yearBuilt': 0,              # yearBuilt as int probably better than float
        'assessorID': 'unknown',
        'legalDescription': 'unknown',
        'subdivision': 'unknown',
        'lotSize': 0.0,
        'ownerOccupied': False,      # boolean default better than 0.0 if it represents True/False
        'features': 'unknown',
        'taxAssessment': 0.0,        # numeric default for tax-related fields
        'propertyTaxes': 0.0,
        'owner': 'unknown',
        'zoning': 'unknown',
        'lastSalePrice': 0.0,
        'addressLine2': 'unknown'
    }, inplace=True)
    
    return df