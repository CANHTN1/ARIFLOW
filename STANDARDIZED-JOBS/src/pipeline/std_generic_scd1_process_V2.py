import argparse
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

from utils.config_loader import load_config_pipeline

def _parse_arguments(args=None):
    parser = argparse.ArgumentParser(description='Spark job for SCD1 master')
    parser.add_argument(
        "--cob_dt",
        required=True,
        type=lambda s: s if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s)
        else (_ for _ in ()).throw(argparse.ArgumentTypeError("cob_dt expects YYYY-MM-DD"))
    )
    parser.add_argument('--cfg_file', type=str, required=True)
    return parser.parse_args(args=args)

def is_diff(col_name):
    left = col(f"new.{col_name}")
    right = col(f"cur.{col_name}")
    return (left != right) | ((left.isNull() & right.isNotNull()) | (left.isNotNull() & right.isNull()))

def main(arguments):
    cob_dt = arguments.cob_dt
    yml_file = arguments.cfg_file

    # Load config
    job_config = load_config_pipeline(yml_file, context_vars={"cob_dt": cob_dt})

    query_sql = job_config["query"]["select_sql"]
    print(query_sql)

    source_cols = [col_cfg["name"] for col_cfg in job_config["columns"]]
    target_table = job_config["target"]["table"]
    primary_key = job_config["target"]["native_key"]

    # Spark session
    spark = SparkSession.builder.appName(job_config["spark"]["job_name"]).enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # Read new data and standardize
    df_new = spark.sql(query_sql)

    if job_config["target"]["standardized"] == 1:
        # Convert empty strings to nulls
        cols_empty_to_null = [col["name"] for col in job_config["columns"] if col.get("null_if_empty", 0) == 1]
        for c in cols_empty_to_null:
            df_new = df_new.withColumn(c, when(col(c) == "", None).otherwise(col(c)))

    # Read current data from the standardized zone
    df_cur = spark.table(target_table).select(source_cols)

    # New records (insert)
    df_insert = df_new.join(df_cur, on=primary_key, how="left_anti")

    # Changed records (update)
    df_joined = df_new.alias("new").join(df_cur.alias("cur"), on=primary_key)
    conditions = [is_diff(col_cfg["name"]) for col_cfg in job_config["columns"] if col_cfg.get("do_not_compare", 0) == 0]

    if conditions:
        filter_expr = conditions[0]
        for cond in conditions[1:]:
            filter_expr = filter_expr | cond
        df_update = df_joined.filter(filter_expr).select("new.*")
    else:
        df_update = spark.createDataFrame([], df_new.schema)

    # Log record counts
    print(f"[INFO] Insert count: {df_insert.count()}")
    print(f"[INFO] Update count: {df_update.count()}")

    # Prepare for merging
    df_update.withColumn("change_type", lit("U")).createOrReplaceTempView("updated_df_view")

    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING updated_df_view AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN UPDATE SET *
    """)

    # Write new data
    df_insert.withColumn("change_type", lit("I")).writeTo(target_table).using("iceberg").append()

    # Write historical data if needed
    write_hist_table = job_config["target"]["write_hist_table"]
    if write_hist_table == '1':
        hist_table = job_config["target"]["hist_table"]
        df_insert_hist = spark.sql(f"SELECT * FROM {target_table} WHERE cob_dt = date'{cob_dt}'")
        df_insert_hist.writeTo(hist_table).using("iceberg").overwritePartitions()

    spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())