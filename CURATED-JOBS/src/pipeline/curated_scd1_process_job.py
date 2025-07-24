import argparse
import re
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from utils.config_loader import load_config_pipeline

# Configure logging
logging.basicConfig(level=logging.INFO)

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
    return (left != right) | ( (left.isNull() & right.isNotNull()) | (left.isNotNull() & right.isNull()) )

def main(arguments):
    cob_dt = arguments.cob_dt
    yml_file = arguments.cfg_file

    # Load config
    job_config = load_config_pipeline(yml_file, context_vars={"cob_dt": cob_dt})

    query_sql = job_config["query"]["select_sql"]
    logging.info(f"Executing query: {query_sql}")

    source_cols = [col_cfg["name"] for col_cfg in job_config["columns"]]
    target_table = job_config["target"]["table"]
    primary_key = job_config["target"]["native_key"]

    # Create Spark session
    spark = SparkSession.builder.appName("scd1_generic_job").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    try:
        # Read new data and normalize
        df_new = spark.sql(query_sql)

        # Read current data from standardized zone
        df_cur = spark.table(target_table).select(source_cols)

        # New records (insert)
        df_insert = df_new.join(df_cur, on=primary_key, how="left_anti")

        # Changed records (update)
        df_joined = df_new.alias("new").join(df_cur.alias("cur"), on=primary_key)
        conditions = [is_diff(col_cfg["name"]) for col_cfg in job_config["columns"] if col_cfg["name"] != "cob_dt"]

        if conditions:
            filter_expr = conditions[0]
            for cond in conditions[1:]:
                filter_expr = filter_expr | cond
            df_update = df_joined.filter(filter_expr).select("new.*")
        else:
            df_update = spark.createDataFrame([], df_new.schema)

        # Log counts
        logging.info(f"Insert count: {df_insert.count()}")
        logging.info(f"Update count: {df_update.count()}")

        # Prepare for merge operation
        df_update.withColumn("change_type", lit("U")).createOrReplaceTempView("updated_df_view")

        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_key])

        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING updated_df_view AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
        """)

        # Write data
        df_insert.withColumn("change_type", lit("I")).writeTo(target_table).using("iceberg").append()

        # Write historical data if required
        write_hist_table = job_config["target"]["write_hist_table"]
        if write_hist_table == '1':
            hist_table = job_config["target"]["hist_table"]
            df_insert_hist = spark.sql(f"SELECT * FROM {target_table} WHERE cob_dt = date'{cob_dt}'")
            df_insert_hist.writeTo(hist_table).using("iceberg").overwritePartitions()

    except Exception as e:
        logging.error(f"Error processing SCD1 job: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())