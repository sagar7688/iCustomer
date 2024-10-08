import pandas as pd
import logging
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


# Configure logging
logging.basicConfig(filename='/tmp/user_interaction.log', level=logging.ERROR)

# Postgres DB Connection String
url = 'postgresql+psycopg2://sagar:sagar1212@localhost:5432/iCustomer'
pgdb_tablename = 'user_interaction_data'
df = ''

#Create Connection Engine
engine = create_engine(url)


#Data Extraction
source_data = '/home/sagar/Desktop/iCustData/data.csv'
try:
    df = pd.read_csv(source_data, parse_dates=['timestamp'])
except FileNotFoundError as e:
    logging.error(f"File not found at: {source_data}")
except pd.errors.ParserError as e:
    logging.error("File could be parsed. Please check CSV is correctly formatted.")
except Exception as e:
    logging.error(f"Unexpected error: {e}")


#Data Cleaning
#Handling missing value with defualt value
df_cleaned = df.fillna(value='Missing')

#Handling missing value by removing the row
#df_cleaned = df.dropna()


#Data Transformation

df_groupby = df_cleaned.groupby(['user_id', 'product_id']).size().reset_index(name='interaction_count')
df_transformed = pd.merge(df_cleaned, df_groupby, on=['user_id', 'product_id'])


#Data Loading
try:
    df_transformed.to_sql(pgdb_tablename, engine, if_exists='append', index=False)
except SQLAlchemyError as e:
    logging.error(f"The connection to database while loading data failed with error : {e}")
except Exception as e:
    logging.error(f"Unexpected error : {e}")

print("Data loaded successfully.")




