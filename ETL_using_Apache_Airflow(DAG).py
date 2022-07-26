# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Irina F.',
    'start_date': days_ago(0),
    'email': ['irina@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# defining the first task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)

# defining the second task
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > \
        /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)

# defining the third task
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='sed -e "s/\s\+/,/g" /home/project/airflow/dags/finalassignment/tollplaza-data.tsv \
        | cut -d"," -f9,10,11 > \
            /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

# defining the fourth task
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='tr -s " " < /home/project/airflow/dags/finalassignment/payment-data.txt \
        | tr " " "," | cut -d"," -f11,12 > \
            /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

# defining the fifth task
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /home/project/airflow/dags/finalassignment/csv_data.csv \
        /home/project/airflow/dags/finalassignment/tsv_data.csv \
        /home/project/airflow/dags/finalassignment/fixed_width_data.csv > \
            /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)

# defining the sixth task
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk -F$',' -v OFS=$',' '{ $4 = toupper($4) }1' /home/project/airflow/dags/finalassignment/extracted_data.csv > \
        /home/project/airflow/dags/finalassignment/staging/transformed-data.csv",
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
