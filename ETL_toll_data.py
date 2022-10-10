# import necessary libraries
from datetime  import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


# define DAG arguments

default_args={
    'owner':"ISBAINE MOHAMED",
    'start_date':days_ago(0),
    'email':['isbainemouhamed@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

# define the DAG

dag=DAG('ETL_toll_data',
default_args=default_args,
description="Apache Airflow Final Assignment",
schedule_interval=timedelta(days=1))

# define the task 'unzip_data'

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -zxvf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging' ,
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -f1,2,3 -d"," /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv --output-delimiter=","' ,
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 -d"   " /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv --output-delimiter ","' ,
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59-61,63-68 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv --output-delimiter ","' ,
    dag=dag,
)


consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv' ,
    dag=dag,
)

transform_data=BashOperator(
    task_id='transform_data',
    bash_command="awk -F',' '{print $1,$2,$3,toupper($4),$5,$6,$7,$8,$9}' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag,
)

#defining task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
