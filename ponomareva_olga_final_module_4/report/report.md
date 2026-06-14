# Отчет по итоговому заданию ETL, модуль 4

# 1. Цель работы

Цель работы - выполнить практические задания по ETL-процессам в Yandex Cloud:

- перенести данные из Managed Service for YDB в Object Storage через Yandex Data Transfer;
- обработать входной CSV-файл размером более 50 MB с помощью Managed Service for Apache Airflow, Yandex Data Processing и PySpark.

Работа выполнялась честно: в отчете указаны только те облачные операции, которые были реально выполнены и подтверждены скриншотами.

## 2. Архитектура 

Фактическая архитектура выполненной части:

1. YDB `transactions_v2` -> Data Transfer -> Object Storage.
2. Object Storage `input/applications.csv` -> Airflow DAG -> временный Data Processing cluster -> PySpark job -> Object Storage marts.

Задания Kafka streaming и DataLens dashboard в данной версии работы не выполнялись.

## 3. Задание 1. YDB -> Object Storage через Data Transfer

### Что сделано

- Создан бакет Object Storage: `etl-module4-olich-2003`.
- Создана YDB database: `etl-module4-ydb`.
- Создана таблица: `transactions_v2`.
- Сгенерирован CSV-файл: `data/generated/transactions_v2.csv`.
- Размер исходного CSV-файла: 32 MB.
- Данные загружены в YDB через YDB CLI командой `import file csv`.
- Загружено строк: `328696`.
- Проверка выполнена YQL-запросом `SELECT COUNT(*)`.
- Создан source endpoint Data Transfer: `ydb-transactions-source`.
- Создан target endpoint Data Transfer: `s3-transactions-target`.
- Создан и запущен transfer: `transfer-ydb-to-s3-transactions-v2` с типом `Копирование`.
- Статус transfer после выполнения: `Завершен`.
- Результат сохранен в Object Storage: `s3://etl-module4-olich-2003/2026/06/12/transactions_v2/`.
- Полученный файл: `part-1781271558-c21f969b.00000.csv`, размер 34.82 MB.

### Проверки

Проверка количества строк в YDB:

```sql
SELECT COUNT(*) AS row_count
FROM transactions_v2;
```

Результат:


| row_count |
| --------- |
| 328696    |


Скриншот проверки в папке screenshots:

Проверка количества строк в transactions_v2

Файл transactions_v2 в Object Storage

## 4. Задание 2. Airflow + Data Processing + PySpark

### Что сделано

- Сгенерирован входной файл `data/generated/applications.csv`, размер 55 MB.
- Количество строк во входном файле: 483364 строк данных.
- Файл загружен в `s3://etl-module4-olich-2003/input/applications.csv`.
- PySpark script загружен в `s3://etl-module4-olich-2003/jobs/process_applications.py`.
- Создан Managed Service for Apache Airflow cluster `airflow-etl-module4`.
- Airflow DAG: `etl_module4_dataproc_pyspark`.
- Data Processing cluster `etl-module4-dataproc` создавался DAG-ом и удалялся после выполнения.
- DAG выполнил три шага: создание Data Processing cluster, запуск PySpark job, удаление cluster.

### Результаты

Output paths:

- `s3://etl-module4-olich-2003/output/applications/batch_result/detail`
- `s3://etl-module4-olich-2003/output/applications/batch_result/mart_by_region_product`
- `s3://etl-module4-olich-2003/output/applications/batch_result/mart_by_day_risk`

### Проверки

- В Airflow все три задачи DAG завершились успешно: `create_dataproc_cluster`, `run_pyspark_applications_job`, `delete_dataproc_cluster`.
- В Object Storage появились выходные папки `detail`, `mart_by_day_risk`, `mart_by_region_product`.
- После выполнения DAG временный Data Processing cluster был удален задачей `delete_dataproc_cluster`.

## 5. Вывод

В ходе работы были выполнены два практических ETL-сценария:

- перенос данных из YDB в Object Storage через Yandex Data Transfer;
- обработки через Airflow DAG с созданием временного Data Processing cluster, запуском PySpark job и удалением cluster после выполнения.



