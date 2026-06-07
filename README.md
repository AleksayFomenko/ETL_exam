# ETL-процессы. Модуль 4 (экзамен)

**Студент:** Фамилия Имя  
**Преподаватель:** Артём Озерков  
**Дисциплина:** ETL-процессы  
**Время выполнения:** 10 часов  

---

## Стек

`Yandex Data Processing` `Apache Airflow` `Apache Kafka` `PySpark`  
`Yandex DataTransfer` `Yandex DataLens` `Yandex Object Storage` `YDB`  
`HiveQL` `Spark SQL` `YQL` `GitHub`

---

## Оценка

| Задание | Описание | Баллы |
|---------|----------|-------|
| 1 | Yandex DataTransfer | 2 |
| 2 | Автоматизация через Apache Airflow | 2 |
| 3 | Apache Kafka + PySpark | 3 |
| 4 | Визуализация в DataLens | 1 |
| | **Итого** | **8** |

---

## Задание 1. Yandex DataTransfer

### 1.1 Создание БД Yandex DataBase

![Создание YDB](screens/task1_ydb_create.png)

### 1.2 Подготовка таблицы transactions_v2

```sql
CREATE TABLE transactions_v2 (
    call_id            Utf8,
    call_time          Datetime,
    client_id          Utf8,
    region_code        Utf8,
    campaign_type      Utf8,
    call_status        Utf8,
    client_response    Utf8,
    duration_sec       Int32,
    follow_up_required Bool,
    PRIMARY KEY (call_id)
);
```

![Таблица в YDB](screens/task1_ydb_table.png)

### 1.3 Создание трансфера в Object Storage

![Настройка трансфера](screens/task1_transfer_setup.png)

### 1.4 Проверка работоспособности

![Результат трансфера](screens/task1_transfer_result.png)

---

## Задание 2. Автоматизация через Apache Airflow

### 2.1 Подготовка инфраструктуры

![Инфраструктура](screens/task2_infra.png)

### 2.2 PySpark-задание

```python
# код PySpark-задания
```

![PySpark задание](screens/task2_pyspark_job.png)

### 2.3 DAG-файл

```python
# код DAG
```

![DAG в Airflow](screens/task2_dag.png)

### 2.4 Результат выполнения

![Результат DAG](screens/task2_dag_result.png)

---

## Задание 3. Apache Kafka + PySpark

### 3.1 Архитектура

![Архитектура](screens/task3_architecture.png)

### 3.2 Чтение топика Kafka

```python
# код чтения топика
```

![Kafka PySpark](screens/task3_pyspark.png)

### 3.3 Разворачивание JSON в плоский вид

```python
# flatten JSON
```

![Плоская таблица](screens/task3_flatten_result.png)

---

## Задание 4. Визуализация в DataLens

![Дашборд DataLens](screens/task4_dashboard.png)

---

## Структура репозитория

```
├── screens/
├── sql/
│   ├── task1_create_table.yql
│   └── task3_flatten.sql
├── src/
│   ├── pyspark_job.py
│   ├── dag.py
│   └── kafka_spark_job.py
└── README.md
```
