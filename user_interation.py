import pandas as pd
import logging
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(filename='/tmp/user_interaction.log', level=logging.ERROR)

# Postgres DB Connection String
pgdb_url = 'postgresql+psycopg2://sagar:sagar1212@localhost:5432/iCustomer'
pgdb_tablename = 'user_interaction_data'

#Create Connection Engine
db_engine = create_engine(pgdb_url)


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

print(df)

#Data Cleaning
#Handling missing value with defualt value
#df_filled = df.fillna(value='Missing')

#Handling missing value by removing the row
#df_cleaned = df.dropna()


#Data Transformation




#Data Loading
try:
    df.to_sql(pgdb_tablename, db_engine, if_exists='append', index=False)
except SQLAlchemyError as e:
    logging.error(f"The connection to database while loading data failed with error : {e}")
except Exception as e:
    logging.error(f"Unexpected error : {e}")

#print("Data loaded successfully.")




