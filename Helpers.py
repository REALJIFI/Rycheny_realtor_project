import pandas as pd



def create_location_dim(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the location dimension table.

    Args:
        df (pd.DataFrame): Cleaned real estate DataFrame.

    Returns:
        pd.DataFrame: Location dimension with unique location records and location_id.
    """
    location_dim = df[['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress',
                       'county', 'longitude', 'latitude', 'addressLine2']].drop_duplicates().reset_index(drop=True)
    location_dim.index.name = 'location_id'
    return location_dim.reset_index()

def create_sales_dim(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the sales dimension table.

    Args:
        df (pd.DataFrame): Cleaned real estate DataFrame.

    Returns:
        pd.DataFrame: Sales dimension with unique sale records and sales_id.
    """
    sales_dim = df[['lastSaleDate', 'lastSalePrice']].drop_duplicates().reset_index(drop=True)
    sales_dim.index.name = 'sales_id'
    return sales_dim.reset_index()

def create_features_dim(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the features dimension table. Converts nested fields to strings.

    Args:
        df (pd.DataFrame): Cleaned real estate DataFrame.

    Returns:
        pd.DataFrame: Features dimension with unique features and features_id.
    """
    df['features'] = df['features'].astype(str)
    df['taxAssessment'] = df['taxAssessment'].astype(str)
    df['propertyTaxes'] = df['propertyTaxes'].astype(str)

    features_dim = df[['bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'features']]\
        .drop_duplicates().reset_index(drop=True)
    features_dim.index.name = 'features_id'
    return features_dim.reset_index()

def create_property_fact(df: pd.DataFrame,
                         sales_dim: pd.DataFrame,
                         location_dim: pd.DataFrame,
                         features_dim: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the property fact table by merging the dimension tables.

    Args:
        df (pd.DataFrame): Cleaned real estate DataFrame.
        sales_dim (pd.DataFrame): Sales dimension table.
        location_dim (pd.DataFrame): Location dimension table.
        features_dim (pd.DataFrame): Features dimension table.

    Returns:
        pd.DataFrame: Property fact table with foreign keys.
    """
    property_fact = df.merge(sales_dim, on=['lastSaleDate', 'lastSalePrice'], how='left')\
                      .merge(location_dim, on=['addressLine1', 'city', 'state', 'zipCode',
                                               'formattedAddress', 'county', 'longitude',
                                               'latitude', 'addressLine2'], how='left')\
                      .merge(features_dim, on=['bedrooms', 'bathrooms', 'squareFootage',
                                               'lotSize', 'features'], how='left')\
                      [['id', 'sales_id', 'location_id', 'features_id', 'yearBuilt',
                        'assessorID', 'legalDescription', 'ownerOccupied', 'propertyType',
                        'taxAssessment', 'propertyTaxes', 'subdivision', 'zoning']]
    return property_fact






