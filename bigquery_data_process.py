import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.utils.dates import days_ago

# Define the POJO class
class BigQueryRecord:
    def __init__(self, field1, field2, field3):
        self.field1 = field1
        self.field2 = field2
        self.field3 = field3

    def to_tuple(self):
        return f"({self.field1}, '{self.field2}', {self.field3})"

# Load environment variables
DATASET_ID = os.getenv('DATASET_ID', 'your_dataset_id')
TABLE_ID = os.getenv('TABLE_ID', 'your_table_id')
PUBSUB_PROJECT_ID = os.getenv('PUBSUB_PROJECT_ID', 'your_project_id')
PUBSUB_TOPIC = os.getenv('PUBSUB_TOPIC', 'your_topic_name')
INSERT_TABLE = os.getenv('INSERT_TABLE', 'another_dataset.another_table')
GCP_CONN_ID = os.getenv('GCP_CONN_ID', 'google_cloud_default')

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='bigquery_pubsub_insert_batch_dag',
    default_args=default_args,
    schedule_interval=None,  # Run manually or set a schedule
) as dag:

    # Task 1: Fetch data from BigQuery
    fetch_data = BigQueryGetDataOperator(
        task_id='fetch_data',
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        max_results=100,  # Adjust as necessary
        selected_fields='field1,field2,field3',  # Specify fields you need
        gcp_conn_id=GCP_CONN_ID,
        location='US',
    )

    # Task 2: Prepare Pub/Sub message and send it, then prepare POJO and insert into BigQuery
    def process_data(**kwargs):
        # Fetch the data from the previous task
        data = kwargs['ti'].xcom_pull(task_ids='fetch_data')

        # Prepare the Pub/Sub message
        pubsub_message = json.dumps(data)
        
        # Send Pub/Sub message
        pubsub_task = PubSubPublishMessageOperator(
            task_id='send_pubsub_message',
            project_id=PUBSUB_PROJECT_ID,
            topic=PUBSUB_TOPIC,
            messages=[{"data": pubsub_message}],
            gcp_conn_id=GCP_CONN_ID,
        )
        pubsub_task.execute(context=kwargs)

        # Prepare POJOs
        pojo_records = [BigQueryRecord(*row) for row in data]

        # Batch insert query
        values = ', '.join([record.to_tuple() for record in pojo_records])
        insert_query = f"""
        INSERT INTO `{INSERT_TABLE}` (field1, field2, field3)
        VALUES {values}
        """

        # Insert data into another BigQuery dataset
        insert_task = BigQueryInsertJobOperator(
            task_id='insert_data',
            configuration={
                "query": {
                    "query": insert_query,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )
        insert_task.execute(context=kwargs)

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )

    # Set task dependencies
    fetch_data >> process_data_task
