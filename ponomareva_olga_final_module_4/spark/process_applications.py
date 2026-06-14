#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch ETL for loan applications CSV/Parquet.")
    parser.add_argument("--input", required=True, help="s3a://<BUCKET_NAME>/input/applications.csv")
    parser.add_argument("--output", required=True, help="s3a://<BUCKET_NAME>/output/applications")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("etl-module4-applications-batch")
        .enableHiveSupport()
        .getOrCreate()
    )

    if args.format == "csv":
        source = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input)
    else:
        source = spark.read.parquet(args.input)

    cleaned = (
        source
        .withColumn("event_ts", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("requested_amount", F.col("requested_amount").cast("double"))
        .withColumn("approved_amount", F.col("approved_amount").cast("double"))
        .withColumn("term_months", F.col("term_months").cast("int"))
        .withColumn("credit_score", F.col("credit_score").cast("int"))
        .withColumn("employee_review_flag", F.col("employee_review_flag").cast("boolean"))
        .withColumn("processing_time_sec", F.col("processing_time_sec").cast("int"))
        .withColumn("is_approved", F.col("decision_status") == F.lit("approved"))
        .withColumn(
            "approved_ratio",
            F.when(F.col("requested_amount") > 0, F.col("approved_amount") / F.col("requested_amount")).otherwise(F.lit(0.0)),
        )
    )

    by_region_product = (
        cleaned
        .groupBy("region_code", "product_type")
        .agg(
            F.count("*").alias("applications_count"),
            F.sum("requested_amount").alias("requested_amount_sum"),
            F.avg("requested_amount").alias("requested_amount_avg"),
            F.sum(F.col("is_approved").cast("int")).alias("approved_count"),
            F.avg("processing_time_sec").alias("processing_time_avg_sec"),
        )
    )

    by_day_risk = (
        cleaned
        .groupBy("event_date", "risk_level", "decision_status")
        .agg(
            F.count("*").alias("applications_count"),
            F.sum("requested_amount").alias("requested_amount_sum"),
            F.avg("credit_score").alias("credit_score_avg"),
        )
    )

    cleaned.write.mode("overwrite").parquet(f"{args.output}/detail")
    by_region_product.write.mode("overwrite").parquet(f"{args.output}/mart_by_region_product")
    by_day_risk.write.mode("overwrite").parquet(f"{args.output}/mart_by_day_risk")

    print("Batch ETL finished")
    print(f"detail: {args.output}/detail")
    print(f"mart_by_region_product: {args.output}/mart_by_region_product")
    print(f"mart_by_day_risk: {args.output}/mart_by_day_risk")
    spark.stop()


if __name__ == "__main__":
    main()

