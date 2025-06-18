"""
Simple Demo DAG
A basic DAG to test Airflow setup before adding GCP operations
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

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
    'simple_demo_dag',
    default_args=default_args,
    description='A simple demo DAG for testing Airflow setup',
    schedule=timedelta(minutes=30),  # Run every 30 minutes for testing
    catchup=False,
    tags=['demo', 'testing'],
)

# Task 1: Start
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Task 2: Print current time
def print_current_time(**context):
    """Print the current time"""
    from datetime import datetime
    current_time = datetime.now()
    print(f"Current time: {current_time}")
    return f"Executed at {current_time}"

print_time = PythonOperator(
    task_id='print_current_time',
    python_callable=print_current_time,
    dag=dag,
)

# Task 3: Generate sample data
def generate_sample_data(**context):
    """Generate sample data for demonstration"""
    import pandas as pd
    from datetime import datetime
    import random
    
    # Create sample data
    data = {
        'id': range(1, 21),
        'name': [f'Item_{i}' for i in range(1, 21)],
        'value': [random.uniform(1.0, 100.0) for _ in range(20)],
        'category': [random.choice(['A', 'B', 'C']) for _ in range(20)],
        'created_at': [datetime.now() for _ in range(20)]
    }
    
    df = pd.DataFrame(data)
    
    # Print summary statistics
    print(f"Generated {len(df)} rows of sample data")
    print(f"Average value: {df['value'].mean():.2f}")
    print(f"Categories: {df['category'].value_counts().to_dict()}")
    
    return f"Generated {len(df)} rows with avg value {df['value'].mean():.2f}"

generate_data = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data,
    dag=dag,
)

# Task 4: Process data
def process_data(**context):
    """Process the generated data"""
    import pandas as pd
    import numpy as np
    
    # Simulate data processing
    print("Processing data...")
    
    # Create some processed data
    processed_data = {
        'processed_id': range(1, 11),
        'processed_value': np.random.normal(50, 10, 10),
        'status': ['completed' for _ in range(10)]
    }
    
    df = pd.DataFrame(processed_data)
    print(f"Processed {len(df)} records")
    print(f"Processing completed successfully")
    
    return "Data processing completed"

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

# Task 5: System information
get_system_info = BashOperator(
    task_id='get_system_info',
    bash_command='echo "System Info:" && uname -a && echo "Python version:" && python --version',
    dag=dag,
)

# Task 6: End
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> print_time >> generate_data >> process_data_task >> get_system_info >> end 