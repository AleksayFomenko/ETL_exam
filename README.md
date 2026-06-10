# ETL-процессы. Модуль 4 (экзамен)

**Студент:** Фоменко Алексей  
**Преподаватель:** Артём Озерков  
**Дисциплина:** ETL-процессы  
**Время выполнения:** 10 часов  

---

## Стек

`Yandex Data Processing` `Apache Airflow` `Apache Kafka` `PySpark`  
`Yandex DataTransfer` `Yandex DataLens` `Yandex Object Storage` `YDB`  
`HiveQL` `Spark SQL` `YQL` `GitHub`

---

## Задание 1. Yandex DataTransfer

### 1.1 Создание БД Yandex DataBase

![Создание YDB](screens/task1_1.png)

### 1.2 Подготовка таблицы transactions_v2

```sql
CREATE TABLE mental_health_survey (
    participant_id                Utf8 NOT NULL,
    age                           Uint32,
    gender                        Utf8,
    country                       Utf8,
    occupation                    Utf8,
    work_hours_per_week           Uint32,
    screen_time_hours             Double,
    sleep_hours                   Double,
    sleep_quality                 Utf8,
    exercise_frequency            Utf8,
    stress_score                  Uint32,
    anxiety_score                 Uint32,
    depression_score              Uint32,
    social_support                Utf8,
    therapy_history               Bool,
    family_history_mental_illness Bool,
    academic_or_job_pressure      Uint32,
    financial_stress_score        Uint32,
    mental_health_risk            Utf8,
    survey_date                   Datetime,
    PRIMARY KEY (participant_id)
);
```

![Таблица в YDB](screens/task1_2.png)

### 1.3 Создание трансфера в Object Storage

![Настройка трансфера](screens/task1_33.png)

### 1.4 Проверка работоспособности

![Результат трансфера](screens/task1_4.png)

---

## Задание 2. Автоматизация через Apache Airflow

### 2.1 Подготовка инфраструктуры

![airflow](screens/task2_1.png)

![bucket](screens/task2_2.png)
### 2.2 PySpark-задание

```python
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("create-table") \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType([
    StructField('Appid', IntegerType(), True),
    StructField('Name', StringType(), True),
    StructField('Type', StringType(), True),
    StructField('ReleaseDate', StringType(), True),
    StructField('Genres', StringType(), True),
    StructField('Developers', StringType(), True),
    StructField('Publishers', StringType(), True),
    StructField('Description', StringType(), True),
    StructField('price', StringType(), True),
    StructField('Thumbnail', StringType(), True),
])

df = spark.read \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .schema(schema) \
    .csv("s3a://bucketforairflow/data/steam.csv")

df.write.mode("overwrite").option("path", "s3a://bucketforairflow/BTCUSTD").saveAsTable("BTCUSTD")
```

### 2.3 DAG-файл

```python
import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)


YC_DP_AZ = 'ru-central1-b'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC7fZqwPCYwjP9dVjC2IaNKZhOyZHcmVDqhaZTwbylhZVRliDhw4QNlDxtNUE1HVgboKeefPdcDB4pfg5+AxRVihSiu21Rixv4SEMWYLyH+NntUotCmmF6OyXkBmcc/BMg/J6gLROrbldjtPzOVF4QYHgX12KUkgVxpUcqg32JpIcjTfl0eL3VEUp2c7nmbPImJCJdsPpDtbSCmkh35sV1oMUmikNOA6MdXQwQv0Eec9jZftug0Qpr+u4gD7EKzsna9LMn/jmhiFLtZSrU6NfUb04YRTZrt15a9KGPOHhrAB9Iv1UVkHzy302432nwahwRcTzBMBrGid+ndDP+KHnUl6MnzRjkWOuROMwhPvukItwJHmDeBKazQORPswsUH6ywnsJz945kEu5px+XdxCE2FO5PGUQwJcvghhlZOS5+vmb+P5rzkv6EvJTZSjWgZJsckaML74wQ0nA0VMERU0eDQOsi4H+5dLbJuJFMlvdl+lHL8ZSucJhMNkgUD7iL2I5s= alex@MacBook-Pro-Aleksej.local'
YC_DP_SUBNET_ID = 'e2la2isn0alfco9839r5'
YC_DP_SA_ID = 'ajeaoikom0jk8615pqg7'
YC_DP_METASTORE_URI = '10.128.0.3'   
YC_BUCKET = 'bucketforairflow'


with DAG(
        'DATA_INGEST',
        schedule='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        masternode_disk_size=20,
        computenode_resource_preset='s2.small',   
        computenode_disk_type='network-ssd',
        computenode_disk_size=20,                  
        computenode_count=1,                       
        computenode_max_hosts_count=3, 
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/create-table.py',
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
```

![DAG в Airflow](screens/task2_3.png)

### 2.4 Результат выполнения

Кластер metastore каждый раз падает при запуске без причин. В логах всё зеленое. Ввиду недоступности кластера metastore, получаем ошибку выполнения spark задания(скрин выше):
спарк пытается подключиться к metastore и падает с max tries.
![Результат DAG](screens/task2_4t.png)

---

## Задание 3. Apache Kafka + PySpark

### 3.1 Архитектура

![Архитектура](screens/task3_1.png)

### 3.2 Чтение топика Kafka

```python
# код чтения топика
```

![Kafka PySpark](screens/task3_2.png)

### 3.3 Разворачивание JSON в плоский вид

```python
# flatten JSON
```

![Плоская таблица](screens/task3_3.png)

---

## Задание 4. Визуализация в DataLens

![Дашборд DataLens](screens/task4_1.png)

---


