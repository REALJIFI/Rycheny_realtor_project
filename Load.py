import psycopg2
import logging
import os
logging.basicConfig(filename='db_insert_errors.log', level=logging.ERROR)


def get_db_connection():
    try:
        conn = psycopg2.connect(
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

    
def insert_location_dim(cursor, location_dim):
    cursor.executemany(
        '''INSERT INTO RYCHENY_REALTOR.location_dim(location_id, addressLine1, city, state, zipCode, formattedAddress,
                        county, longitude, latitude, addressLine2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
        location_dim.values.tolist()
    )

def insert_sales_dim(cursor, sales_dim):
    cursor.executemany(
        '''INSERT INTO RYCHENY_REALTOR.sales_dim(sales_id, lastSaleDate, lastSalePrice)
            VALUES (%s, %s, %s)''',
        sales_dim[['sales_id', 'lastSaleDate', 'lastSalePrice']].values.tolist()
    )

def insert_features_dim(cursor, features_dim):
    cursor.executemany(
        '''INSERT INTO RYCHENY_REALTOR.features_dim(features_id, bedrooms, bathrooms, squareFootage, lotSize, features)
            VALUES (%s, %s, %s, %s, %s, %s)''',
        features_dim[['features_id', 'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'features']].values.tolist()
    )

def insert_property_fact_table(cursor, property_fact_table):
    for _, row in property_fact_table.iterrows():
        try:
            cursor.execute('SELECT 1 FROM RYCHENY_REALTOR.location_dim WHERE id = %s', (row['location_id'],))
            if cursor.fetchone() is None:
                raise ValueError(f"location_id {row['location_id']} does not exist in location_dim")

            cursor.execute(
                '''INSERT INTO RYCHENY_REALTOR.property_fact_table(id, sales_id, location_id, features_id, yearBuilt,
                    assessorID, legalDescription, ownerOccupied, propertyType, taxAssessment, propertyTaxes, subdivision, zoning)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (row['id'], row['sales_id'], row['location_id'], row['features_id'], row['yearBuilt'], row['assessorID'],
                 row['legalDescription'], row['ownerOccupied'], row['propertyType'], row['taxAssessment'],
                 row['propertyTaxes'], row['subdivision'], row['zoning'])
            )
        except Exception as e:
            logging.error(f"Error inserting row with id {row['id']}: {e}")
