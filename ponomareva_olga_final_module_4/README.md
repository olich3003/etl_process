# итоговое задание ETL 4

Выполнены задания 1 и 2

## Архитектура

YDB -> Data Transfer -> Object Storage

Object Storage input CSV -> Airflow DAG -> временный Data Processing cluster -> PySpark job -> Object Storage output

## Описание файлов

- `sql/ydb/01_create_transactions_v2.yql` - создание таблицы `transactions_v2`.
- `sql/ydb/02_check_transactions_v2.yql` - проверки количества строк и распределений.
- `scripts/generate_transactions_v2.py` - генератор CSV 30+ MB для YDB.
- `scripts/create_iam_token.py` - получение IAM token из ключа сервисного аккаунта.
- `scripts/load_transactions_ydb.py` - шаблон загрузки CSV в YDB через Python SDK.
- `scripts/generate_applications_csv.py` - генератор CSV 50+ MB для batch PySpark.
- `spark/process_applications.py` - batch PySpark обработка CSV.
- `airflow/dags/dataproc_pyspark_etl_dag.py` - DAG Airflow для создания Data Processing cluster, запуска PySpark job и удаления cluster.
- `configs/env.example` - список параметров, которые нужно заменить на свои.
- `report/report.md` - отчет по выполненным заданиям 1 и 2.

Файлы `service-account-key.json`, `iam-token.txt` и приватные SSH-ключи не публикуются в GitHub и указаны в `.gitignore`.

## Результаты

Задание 1:

- YDB table: `transactions_v2`.
- Количество строк в YDB: `328696`.
- Object Storage result: `s3://etl-module4-olich-2003/2026/06/12/transactions_v2/`.
- Размер выгруженного файла: 34.82 MB.

Задание 2:

- Input file: `s3://etl-module4-olich-2003/input/applications.csv`, 55 MB.
- PySpark script: `s3://etl-module4-olich-2003/jobs/process_applications.py`.
- Airflow DAG: `etl_module4_dataproc_pyspark`.
- Output:
  - `s3://etl-module4-olich-2003/output/applications/batch_result/detail`
  - `s3://etl-module4-olich-2003/output/applications/batch_result/mart_by_region_product`
  - `s3://etl-module4-olich-2003/output/applications/batch_result/mart_by_day_risk`

## Проверки

Для задания 1 выполнен YQL-запрос:

```sql
SELECT COUNT(*) AS row_count
FROM transactions_v2;
```

Результат: `328696`.

Для задания 2 проверено:

- DAG Airflow завершился успешно;
- задачи `create_dataproc_cluster`, `run_pyspark_applications_job`, `delete_dataproc_cluster` зеленые;
- в Object Storage есть папки `detail`, `mart_by_region_product`, `mart_by_day_risk`.

