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

![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/f38df39acad7295b6b8a0ebadd13240a427d0feb/images/1-start_airflow.png)

- Download the dataset from the source to the destination mentioned below.
Note: While downloading the file in the terminal use the sudo command before the command used to download the file.

Source : https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz
Destination : /home/project/airflow/dags/finalassignment

![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4446592d035cd1a72477a7878d6ef654d1eed291/images/2-download%20the%20dataset.png)

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
![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4446592d035cd1a72477a7878d6ef654d1eed291/images/3-move%20data%20to%20project%20folder.png)
### creating DAG File

- Define DAG arguments
Let's define the DAG arguments as per the following details:

![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4446592d035cd1a72477a7878d6ef654d1eed291/images/4-dagsparameters.png)

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

![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/f38df39acad7295b6b8a0ebadd13240a427d0feb/images/submit_dag.png)

Now let's Unpause the dag :

```bash
airflow dags unpause ETL_toll_data
```

![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/f38df39acad7295b6b8a0ebadd13240a427d0feb/images/unpause_dag.png)

And Finally let's check if everything is OK on the Airflow web interface console


![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/f38df39acad7295b6b8a0ebadd13240a427d0feb/images/dag_runs.png)

## Part2: Creating Streaming Data Pipelines using Kafka

You are a data engineer at a data analytics consulting company. You have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. As a vehicle passes a toll plaza, the vehicle's data like vehicle_id,vehicle_type,toll_plaza_id and timestamp are streamed to Kafka. Your job is to create a data pipe line that collects the streaming data and loads it into a database.

### Objectives
In this part we will create a streaming data pipe by performing these steps:

* Start a MySQL Database server.
* Create a table to hold the toll data.
* Start the Kafka server.
* Install the Kafka python driver.
* Install the MySQL python driver.
* Create a topic named toll in kafka.
* Download streaming data generator program.
* Customize the generator program to steam to toll topic.
* Download and customise streaming data consumer.
* Customize the consumer program to write into a MySQL database table.
* Verify that streamed data is being collected in the database table.

### Prepare the lab environment :

Before we start the project, we have the following steps to set up the workspace:

- Step 1: Download Kafka

```bash
wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.12-2.8.0.tgz
```

- Step 2: Extract Kafka

```bash
tar -xzf kafka_2.12-2.8.0.tgz
```

- Step 3: Start MySQL server

```bash
start_mysql
```

- Step 4: Connect to the mysql server, using the command below. Make sure you use the password given to you when the MySQL server starts.

```bash
mysql --host=127.0.0.1 --port=3306 --user=root --password=<your-password-here>
```

- Step 5: Create a database named tolldata

```mysql
create database tolldata;
```

- Step 6: Create a table named livetolldata with the schema to store the data generated by the traffic simulator

```mysql
use tolldata;

create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);
```
This is the table where we would store all the streamed data that comes from kafka. Each row is a record of when a vehicle has passed through a certain toll plaza along with its type and anonymized id.

- Step 7: Disconnect from MySQL server

```mysql
exit
```

- Step 8: Install the python module kafka-python using the pip3 command
```bash
python3 -m pip install kafka-python
```
This python module will help us to communicate with kafka server. It can used to send and receive messages from kafka.

- Step 9: Install the python module mysql-connector-python using the pip3 command
```bash
python3 -m pip install mysql-connector-python 
```
This python module will help us to interact with mysql server.


### Configure Kafka

- Step1: Start Zookeeper
Start zookeeper server with the following command

```bash
cd kafka_2.12-2.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties
```
![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4e1b86b61bad27b89277eb39e01e84cf1df5d9b1/images/start_zookeeper.png)
 
- Step2: Start the Kafka broker service
Start kafka server with the following command

```bash
cd kafka_2.12-2.8.0
bin/kafka-server-start.sh config/server.properties
```
 ![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4e1b86b61bad27b89277eb39e01e84cf1df5d9b1/images/start_kafka.png)

- Step3: Create a topic called Toll
We need to create a topic before you can start to post messages.

To create a topic named *toll*, start a new terminal and run the command below.

```bash
cd kafka_2.12-2.8.0
bin/kafka-topics.sh --create --topic toll --bootstrap-server localhost:9092
```
![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4e1b86b61bad27b89277eb39e01e84cf1df5d9b1/images/create_toll_topic.png)

- Step4: Create a raffic simulator
We need to create a message generator, create a file called `toll_traffic_generator.py` in put the code below inside it:

```python
"""
Top Traffic Simulator
"""
from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'toll'

VEHICLE_TYPES = ("car", "car", "car", "car", "car", "car", "car", "car",
                 "car", "car", "car", "truck", "truck", "truck",
                 "truck", "van", "van")
for _ in range(100000):
    vehicle_id = randint(10000, 10000000)
    vehicle_type = choice(VEHICLE_TYPES)
    now = ctime(time())
    plaza_id = randint(4000, 4010)
    message = f"{now},{vehicle_id},{vehicle_type},{plaza_id}"
    message = bytearray(message.encode("utf-8"))
    print(f"A {vehicle_type} has passed by the toll plaza {plaza_id} at {now}.")
    producer.send(TOPIC, message)
    sleep(random() * 2)

```
```bash
python3 toll_traffic_generator.py
```
![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4e1b86b61bad27b89277eb39e01e84cf1df5d9b1/images/simulator_output.png)

- Step5: Create a consumer (traffic reader)
We need to create a message consumer, create a file called `streaming_data_reader.py` in put the code below inside it:

```python
"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

TOPIC='toll'
DATABASE = 'tolldata'
USERNAME = 'root'
PASSWORD = 'MTM2MjEtbW9oYW1l'

print("Connecting to the database")
try:
    connection = mysql.connector.connect(host='localhost', database=DATABASE, user=USERNAME, password=PASSWORD)
except Exception:
    print("Could not connect to database. Please check credentials")
else:
    print("Connected to database")
cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    # Extract information from kafka

    message = msg.value.decode("utf-8")

    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table

    sql = "insert into livetolldata values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
connection.close()


```
```bash
streaming_data_reader.py
```
![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4e1b86b61bad27b89277eb39e01e84cf1df5d9b1/images/data_reader_output.png)


### Health Chekc of the data Pipeline

We have done all the steps till here correctly, the streaming toll data would get stored in the table livetolldata.

Let's List the top 10 rows in the table livetolldata.

```mysql
use tolldata;
select * from livetolldata limit 10;
```

SCREEN SQL REQUEST
![image](https://github.com/isbainemohamed/Data-Pipelines-Kafka-Airflow/blob/4e1b86b61bad27b89277eb39e01e84cf1df5d9b1/images/output_rows.png)

# Conclusion

Congratulations on completing this ETL and Data Pipelines with Airflow and Kafka ! I hope you enjoyed it, please leave me any question in the email below
isbainemouhamed@gmail.com






