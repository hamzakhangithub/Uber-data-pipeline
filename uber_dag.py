from airflow import DAG  
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import pandas as pd 
import numpy as np 
import boto3
from botocore.exceptions import NoCredentialsError



def upload_to_s3(df, bucket_name, file_name):
    try:
        # Initialize a session using AWS S3 credentials
        session = boto3.Session(
            aws_access_key_id='AKIA6FK6C3VFBTCVQN6Q',
            aws_secret_access_key='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'
        )

        # Convert DataFrame to CSV and save to S3
        csv_buffer = df.to_csv(index=False).encode('utf-8')
        s3_resource = session.resource('s3')
        s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer)

        print(f"Data uploaded to S3: s3://{bucket_name}/{file_name}")
        
    except NoCredentialsError:
        print("AWS credentials not available")


def load_transform_data():

    df = pd.read_csv('https://raw.githubusercontent.com/darshilparmar/uber-etl-pipeline-data-engineering-project/main/data/uber_data.csv')

    if df.empty:
        print("Data not downloaded.")
        
    
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    bucket_name = 'uber-pipeline'
    file_name = 'cleaned_uber_data.csv'
    upload_to_s3(df, bucket_name, file_name)

    return df

def trigger_glue_crawler():
    try:
        client = boto3.client('glue', region_name='ap-south-1')  # Change region as needed
        response = client.start_crawler(Name='uber-crawler')
        print(response)
    except Exception as e:
        print(f"Error triggering Glue Crawler: {e}")

def run_athena_queries():
    queries = [
        """
        SELECT * FROM uber_pipeline where passenger_count>3
        """,
        """
        SELECT MAX(total_amount) as "Highest Total Amount" FROM uber_pipeline
        """,
        """
        SELECT COUNT(*) AS total_trips
        FROM uber_pipeline
        """,
        """SELECT AVG(trip_distance) AS avg_distance FROM uber_pipeline""",
        """SELECT SUM(tip_amount) AS total_tips FROM uber_pipeline""",
        """SELECT AVG(fare_amount / passenger_count) AS avg_fare_per_passenger FROM uber_pipeline;""",
        """SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour, COUNT(*) AS trip_count FROM uber_pipeline
           GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
           ORDER BY trip_count DESC
           LIMIT 1;
        """,
        """SELECT DATE(tpep_pickup_datetime) AS trip_day, SUM(total_amount) AS total_earnings
           FROM uber_pipeline
           GROUP BY DATE(tpep_pickup_datetime)
           ORDER BY trip_day;
        """

        
        # Add more queries as needed...
    ]

    try:
        athena_client = boto3.client('athena', region_name='ap-south-1')  # Change region as needed

        for i, query in enumerate(queries, start=1):
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': 'uber-pipe-database'},  # Change to your database name
                ResultConfiguration={'OutputLocation': f's3://uber-pipeline-queries/athena-output/query_{i}/'}  # Change S3 path as needed
            )
            print(f"Athena Query {i} Execution ID: {response['QueryExecutionId']}")

    except Exception as e:
        print(f"Error running Athena queries: {e}")







def success_message():
    print("Data Pipeline successful.")





default_args = {
    'owner': 'hkhann',
    'depends_on_past':False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email':'ihamzakhan89@gmail.com',
    'email_on_failure':True,
    'email_on_retry':False
}

dag = DAG(
    dag_id = 'Uber_data',
    default_args = default_args,
    description = "A DAG for uber data.",
    start_date = datetime(2023,10,17),
    schedule_interval = timedelta(days=1),
)

load_transform_task = PythonOperator(
    task_id='load_and_transform',
    python_callable=load_transform_data,
    dag=dag,
)

trigger_glue_crawler_task = PythonOperator(
    task_id='trigger_glue_crawler',
    python_callable=trigger_glue_crawler,
    dag=dag,
)

run_athena_query_task = PythonOperator(
    task_id='run_athena_query',
    python_callable=run_athena_queries,
    retries=3,
    dag=dag,
)



message = PythonOperator(
    task_id = 'success_message',
    python_callable = success_message,
    dag = dag,
)

    

load_transform_task >> trigger_glue_crawler_task >> run_athena_query_task >> message





