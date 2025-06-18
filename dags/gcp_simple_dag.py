"""
Simple GCP DAG Example
This DAG demonstrates basic GCP operations using Apache Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSListObjectsOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'gcp_simple_dag',
    default_args=default_args,
    description='A simple DAG demonstrating GCP operations',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['gcp', 'bigquery', 'cloud-storage'],
)

# Task 1: Start
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Task 2: Create a GCS bucket
create_bucket = GCSCreateBucketOperator(
    task_id='create_gcs_bucket',
    bucket_name='airflow-demo-bucket-{{ ds_nodash }}',
    project_id='your-gcp-project-id',  # Replace with your actual project ID
    dag=dag,
)

# Task 3: Create BigQuery dataset
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id='airflow_demo_dataset',
    project_id='your-gcp-project-id',  # Replace with your actual project ID
    dag=dag,
)

# Task 4: Create BigQuery table
create_table = BigQueryCreateEmptyTableOperator(
    task_id='create_bigquery_table',
    dataset_id='airflow_demo_dataset',
    table_id='sample_data',
    project_id='your-gcp-project-id',  # Replace with your actual project ID
    schema_fields=[
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    ],
    dag=dag,
)

# Task 5: Python function to generate sample data
def generate_sample_data(**context):
    """Generate sample data for demonstration"""
    import pandas as pd
    from datetime import datetime
    
    # Create sample data
    data = {
        'id': range(1, 101),
        'name': [f'Item_{i}' for i in range(1, 101)],
        'value': [i * 1.5 for i in range(1, 101)],
        'created_at': [datetime.now() for _ in range(100)]
    }
    
    df = pd.DataFrame(data)
    
    # Save to CSV (this would typically be uploaded to GCS)
    df.to_csv('/tmp/sample_data.csv', index=False)
    
    print(f"Generated {len(df)} rows of sample data")
    return f"Generated {len(df)} rows"

generate_data = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data,
    dag=dag,
)

# Task 6: Upload data to BigQuery (simulated)
def upload_to_bigquery(**context):
    """Simulate uploading data to BigQuery"""
    print("Simulating data upload to BigQuery...")
    print("In a real scenario, you would use GCSToBigQueryOperator")
    return "Data uploaded successfully"

upload_data = PythonOperator(
    task_id='upload_to_bigquery',
    python_callable=upload_to_bigquery,
    dag=dag,
)

# Task 7: Run a BigQuery query
run_query = BigQueryInsertJobOperator(
    task_id='run_bigquery_query',
    configuration={
        'query': {
            'query': '''
                SELECT 
                    COUNT(*) as total_records,
                    AVG(value) as avg_value,
                    MIN(created_at) as earliest_record,
                    MAX(created_at) as latest_record
                FROM `your-gcp-project-id.airflow_demo_dataset.sample_data`
            ''',
            'useLegacySql': False,
        }
    },
    project_id='your-gcp-project-id',  # Replace with your actual project ID
    dag=dag,
)

# Task 8: List GCS objects
list_gcs_objects = GCSListObjectsOperator(
    task_id='list_gcs_objects',
    bucket='airflow-demo-bucket-{{ ds_nodash }}',
    dag=dag,
)

# Task 9: Cleanup - Delete bucket
delete_bucket = GCSDeleteBucketOperator(
    task_id='delete_gcs_bucket',
    bucket_name='airflow-demo-bucket-{{ ds_nodash }}',
    force_delete=True,
    dag=dag,
)

# Task 10: End
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> create_bucket >> create_dataset >> create_table
create_table >> generate_data >> upload_data >> run_query
run_query >> list_gcs_objects >> delete_bucket >> end 