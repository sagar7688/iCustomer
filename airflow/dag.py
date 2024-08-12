from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Define default arguments
default_args = {
    'owner': 'airflow'
}

# Define the DAG
with DAG(
    'user_interaction_dag',
    default_args=default_args,
    description='Ingest user interaction data in Postgres',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define task functions
    def data_extraction():
        source_data = '/home/sagar/Desktop/iCustData/data.csv'
        try:
            df = pd.read_csv(source_data, parse_dates=['timestamp'])
        except FileNotFoundError as e:
            logging.error(f"File not found at: {source_data}")
        except pd.errors.ParserError as e:
            logging.error("File could be parsed. Please check CSV is correctly formatted.")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def data_cleaning():
        source_data = '/home/sagar/Desktop/iCustData/data.csv'
        df = pd.read_csv(source_data, parse_dates=['timestamp'])
        df.fillna(value='Missing')
        df.to_csv('/home/sagar/Desktop/iCustData/data_cleaned.csv', index=False)

    def data_transformation():
        cleaned_data = '/home/sagar/Desktop/iCustData/data_cleaned.csv'
        df = pd.read_csv(cleaned_data)

        df.to_csv('/home/sagar/Desktop/iCustData/data_transformed.csv', index=False)

    def data_loading():
        transformed_data = '/home/sagar/Desktop/iCustData/data_transformed.csv'
        df = pd.read_csv(transformed_data)

        pgdb_url = 'postgresql+psycopg2://sagar:sagar1212@localhost:5432/iCustomer'
        pgdb_tablename = 'user_interaction_data'

        # Create Connection Engine
        db_engine = create_engine(pgdb_url)

        try:
            df.to_sql(pgdb_tablename, db_engine, if_exists='append', index=False)
        except SQLAlchemyError as e:
            logging.error(f"The connection to database while loading data failed with error : {e}")
        except Exception as e:
            logging.error(f"Unexpected error : {e}")

    # Define tasks
    read_source_file = PythonOperator(
        task_id='Data_Extraction',
        python_callable=data_extraction,
    )

    clean_data = PythonOperator(
        task_id='Data_Cleaning',
        python_callable=data_cleaning
    )

    transform_data = PythonOperator(
        task_id='Data_Transformation',
        python_callable=data_transformation,
    )

    load_data = PythonOperator(
        task_id='Data_Loading',
        python_callable=data_loading,
    )

    #Set task dependencies
    read_source_file >> clean_data >> transform_data >> load_data

