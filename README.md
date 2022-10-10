# Data-Pipelines-Kafka-Airflow
In this project we will build an ETL,  and create a pipeline and upload the data into a database

## Part 1:


You are a data engineer at a data analytics consulting company. You have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. Each highway is operated by a different toll operator with a different IT setup that uses different file formats. Your job is to collect data available in different formats and consolidate it into a single file.

### Objectives
In this assignment you will author an Apache Airflow DAG that will:

Extract data from a csv file
Extract data from a tsv file
Extract data from a fixed width file
Transform the data
Load the transformed data into the staging area

### Preparing the workspace


- Start Apache Airflow:
```bash
start_airflow
```

SCREEN 1 

- Download the dataset from the source to the destination mentioned below.
Note: While downloading the file in the terminal use the sudo command before the command used to download the file.

Source : https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz
Destination : /home/project/airflow/dags/finalassignment

SCREEN 2

- Create a directory structure for staging area as follows
/home/project/airflow/dags/finalassignment/staging.

Firstly enter the command cd airflow/dags in the terminal to change the directory to the /home/project/airflow/dags .

```bash
cd airflow/dags
```

Then ,next enter the below given commands to create the directories finalassignment and staging

```bash
sudo mkdir finalassignment
cd finalassignment
sudo mkdir staging
cd staging
sudo mv /home/project/tolldata.tgz /home/project/airflow/dags/finalassignment
```
SCREEN 3
### creating DAG File

- Define DAG arguments
Let's define the DAG arguments as per the following details:

SCREEN 4

Here is the code to define DAG arguments

```python
default_args={
    'owner':"ISBAINE MOHAMED",
    'start_date':today,
    'email':['isbainemouhamed@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}
```

SCREEN dag_args

- Define DAG arguments

```python
dag=DAG('ETL_toll_data',
default_args=default_args,
description="Apache Airflow Final Assignment",
schedule_interval=timedelta(days=1))
```

- Create a task to unzip data

We will reate a task named unzip_data.

We will use the downloaded data from the url given before and uncompress it into the destination directory.

```python
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -zxvf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging' ,
    dag=dag,
)
```

- Create a task to extract data from csv file
Create a task named extract_data_from_csv.

This task should extract the fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv file and save them into a file named csv_data.csv

```python
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -f1,2,3 -d"," /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv --output-delimiter=","' ,
    dag=dag,
)
```

- Create a task to extract data from tsv file

Create a task named extract_data_from_tsv.

This task should extract the fields Number of axles, Tollplaza id, and Tollplaza code from the tollplaza-data.tsv file and save it into a file named tsv_data.csv.

```python

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 -d"   " /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv --output-delimiter ","' ,
    dag=dag,
)
```

- Create a task to extract data from fixed width file
We will create a task named extract_data_from_fixed_width.

This task should extract the fields Type of Payment code, and Vehicle Code from the fixed width file payment-data.txt and save it into a file named fixed_width_data.csv.

```python

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59-61,63-68 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv --output-delimiter ","' ,
    dag=dag,
)
```

- Create a task to consolidate data extracted from previous tasks
we will create a task named consolidate_data.

This task should create a single csv file named extracted_data.csv by combining data from

csv_data.csv
tsv_data.csv
fixed_width_data.csv

The final csv file should use the fields in the order given below:

Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles, Tollplaza id, Tollplaza code, Type of Payment code, and Vehicle Code

```python
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='cut -d" " -f17,18 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv' ,
    dag=dag,
)
```
paste command merges lines of files.

Example : paste file1 file2 > newfile

The above command merges the columns of the files file1 and file2 and sends the output to newfile.

- Transform and load the data

Create a task named transform_data.

This task should transform the vehicle_type field in extracted_data.csv into capital letters and save it into a file named transformed_data.csv in the staging directory.



```python
transform_data=BashOperator(
    task_id='transform_data',
    bash_command="awk -F',' '{print $1,$2,$3,toupper($4),$5,$6,$7,$8,$9}' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag,)
```
## Submit the dag file

Let's put the dag file in */home/project/ariflow/dags* directory and check dag list using the command

```bash
airflow dags list
```

SCREEN SUCCESS

Now let's Unpause the dag :

```bash
airflow dags unpause ETL_toll_data
```

SCREEN UNPAUSED

And Finally let's check if everything is OK on the Airflow web interface console

SCREEN UI







