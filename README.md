# iCustomer

**Table of contents**

- [Data Ingestion and ETL](#data-ingestion-and-etl)
- [Data Pipeline with Apache Airflow](#data-pipeline-with-apache-airflow)
- [Data Storage and Retrieval](#data-storage-and-retrieval)



## Data Ingestion and ETL

Data Ingestion python script 'user_interaction.py' requires pandas and SQLAlchemy modules which can be installed as below : 
pip install pandas sqlalchemy

Database used in this use case is Postgres. The data loaded sql file is 'pg_dataload.sql'


## Data Pipeline with Apache Airflow

Setuo airflow standalone using instructions here : https://airflow.apache.org/docs/apache-airflow/stable/start.html

The dag file 'af_user_interaction.py' with dag name 'user_insteraction_data_dag' consists of 4 tasks :
1. Data_Extraction
2. Data_Cleaning
3. Data_Transformation
4. Data_Loading


## Data Storage and Retrieval

Queries for Data Retrieval tasks mentioned below are added in folder DataRetrieval:
1. Total number of interactions per day
2. Top 5 users by the number of interactions
3. Most interacted products based on the number of interactions


