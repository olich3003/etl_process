from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)


FOLDER_ID = Variable.get("YC_FOLDER_ID", default_var="<FOLDER_ID>")
SERVICE_ACCOUNT_ID = Variable.get("YC_SERVICE_ACCOUNT_ID", default_var="<SERVICE_ACCOUNT_ID>")
SUBNET_ID = Variable.get("YC_SUBNET_ID", default_var="<SUBNET_ID>")
SECURITY_GROUP_ID = Variable.get("YC_SECURITY_GROUP_ID", default_var="<SECURITY_GROUP_ID>")
BUCKET_NAME = Variable.get("YC_BUCKET_NAME", default_var="<BUCKET_NAME>")
ZONE = Variable.get("YC_ZONE", default_var="ru-central1-b")
CONNECTION_ID = Variable.get("YC_AIRFLOW_CONN_ID", default_var="yandexcloud_default")
SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY", default_var="")
DATAPROC_VERSION = Variable.get("YC_DATAPROC_VERSION", default_var="2.1")

CLUSTER_NAME = "etl-module4-dataproc"
INPUT_PATH = f"s3a://{BUCKET_NAME}/input/applications.csv"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/output/applications/batch_result"
PYSPARK_SCRIPT = f"s3a://{BUCKET_NAME}/jobs/process_applications.py"


with DAG(
    dag_id="etl_module4_dataproc_pyspark",
    description="Create Data Processing cluster, run PySpark ETL, delete cluster.",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "dataproc", "pyspark", "module4"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        folder_id=FOLDER_ID,
        cluster_name=CLUSTER_NAME,
        cluster_description="Temporary cluster for ETL module 4 exam task",
        cluster_image_version=DATAPROC_VERSION,
        services=("HDFS", "YARN", "SPARK"),
        s3_bucket=BUCKET_NAME,
        zone=ZONE,
        subnet_id=SUBNET_ID,
        service_account_id=SERVICE_ACCOUNT_ID,
        security_group_ids=[SECURITY_GROUP_ID],
        ssh_public_keys=[SSH_PUBLIC_KEY],
        masternode_resource_preset="s2.small",
        masternode_disk_type="network-ssd",
        masternode_disk_size=32,
        datanode_resource_preset="s2.small",
        datanode_disk_type="network-ssd",
        datanode_disk_size=64,
        datanode_count=1,
        computenode_count=0,
        connection_id=CONNECTION_ID,
        labels={"project": "etl-module4"},
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark_applications_job",
        cluster_id=create_cluster.output,
        main_python_file_uri=PYSPARK_SCRIPT,
        args=[
            "--input",
            INPUT_PATH,
            "--output",
            OUTPUT_PATH,
            "--format",
            "csv",
        ],
        name="etl-module4-applications-batch",
        connection_id=CONNECTION_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_id=create_cluster.output,
        connection_id=CONNECTION_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> run_pyspark >> delete_cluster
