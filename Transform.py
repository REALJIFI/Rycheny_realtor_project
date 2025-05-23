import pandas as pd

def clean_real_estate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the real estate DataFrame by replacing missing values with defaults.

    Args:
        df (pd.DataFrame): Raw DataFrame with possible missing values.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    df.fillna({
        'bathrooms': 0.0,
        'bedrooms': 0.0,
        'squareFootage': 0.0,
        'county': 'unknown',
        'propertyType': 'unknown',
        'yearBuilt': 0.0,
        'assessorID': 'unknown',
        'legalDescription': 'unknown',
        'subdivision': 'unknown',
        'lotSize': 0.0,
        'ownerOccupied': 0.0,
        'features': 'unknown',
        'taxAssessment': 'unknown',
        'propertyTaxes': 'unknown',
        'owner': 'unknown',
        'zoning': 'unknown',
        'lastSalePrice': 0.0,
        'addressLine2': 'unknown'
    }, inplace=True)
    
    return df

# Example usage
if __name__ == "__main__":
    # This is just for testing the transform module directly
    sample_df = pd.DataFrame({
        'bathrooms': [1.0, None],
        'bedrooms': [2.0, 3.0],
        'county': [None, 'Orange']
    })
    
    cleaned_df = clean_real_estate_data(sample_df)
    print(cleaned_df)
