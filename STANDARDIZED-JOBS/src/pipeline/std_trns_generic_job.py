import argparse
import re
from pyspark.sql import SparkSession
from utils.config_loader import load_config_pipeline

def _parse_arguments(args=None):
    parser = argparse.ArgumentParser(description='Spark SCD2 Job')
    parser.add_argument(
        "--cob_dt",
        required=True,
        type=lambda s: s if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s)
        else (_ for _ in ()).throw(argparse.ArgumentTypeError("cob_dt expects YYYY-MM-DD"))
    )
    parser.add_argument('--cfg_file', type=str, required=True)
    return parser.parse_args(args=args)

def main(arguments):
    cob_dt = arguments.cob_dt
    yml_file = arguments.cfg_file

    # Load configuration
    job_config = load_config_pipeline(yml_file, context_vars={"cob_dt": cob_dt})
    job_name = job_config["spark"]["job_name"]
    target_table = job_config["target"]["table_name"]
    preload = job_config["target"].get("preload", 0)
    query_sql = job_config["target"]["query_sql"]
    write_mode = job_config["target"].get("write_mode", "append")

    # Initialize Spark session
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # Optional preload
    if preload == 1:
        preload_sql = job_config["target"]["preload_sql"]
        print(f"[INFO] Running preload SQL: {preload_sql}")
        spark.sql(preload_sql)

    # Execute main query
    print("[INFO] Main query statement:", query_sql)
    df = spark.sql(query_sql)

    # Write to Iceberg table
    if write_mode == "overwrite":
        print("[INFO] Overwriting partition in table:", target_table)
        df.writeTo(target_table).using("iceberg").overwritePartitions()
    else:
        print("[INFO] Appending to table:", target_table)
        df.writeTo(target_table).using("iceberg").append()

    # Stop Spark session
    spark.stop()
    print("[INFO] Job completed successfully.")

if __name__ == "__main__":
    main(arguments=_parse_arguments())