import requests
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

# Database connection parameters
db_url = 'postgresql+psycopg2://sagar:sagar1212@localhost:5432/iCustomer'

# Create an SQLAlchemy engine
engine = create_engine(db_url)

# URL of the remote CSV file
raw_url = 'https://raw.githubusercontent.com/sagar7688/iCustomer/88f71ccc32f4a06a405f7abccccd9fe644658bf0/sourcefile/data.csv'

# Fetch the CSV file using requests
response = requests.get(raw_url)
print(response)

# Check if the request was successful
if response.status_code == 200:
    # Read the content and load it into a pandas DataFrame
    csv_data = StringIO(response.text)
    df = pd.read_csv(csv_data)

    # Display the first few rows of the DataFrame
    print(df.head())
else:
    print(f"Failed to fetch the CSV file. Status code: {response.status_code}")

# Define the table name
table_name = 'user_interaction_data'

# Load data into PostgreSQL using the to_sql method
df.to_sql(table_name, engine, if_exists='append', index=False)
df.to_sql(table_name, engine, if_exists='append', index=False)

print("Data loaded successfully.")




