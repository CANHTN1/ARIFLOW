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

    # Load job configuration
    job_config = load_config_pipeline(yml_file, context_vars={"cob_dt": cob_dt})
    job_name = job_config["spark"]["job_name"]

    # Initialize Spark session
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()

    # Set Spark configurations
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # Extract target table and SQL query from configuration
    target_table = job_config["target"]["table_name"]
    preload = job_config["target"]["preload"]
    query_sql = job_config["target"]["query_sql"]
    
    # Execute preload SQL if specified
    if preload == '1':
        preload_sql = job_config["target"]["preload_sql"]
        spark.sql(preload_sql)
    
    # Execute the main query and write to the target table
    df = spark.sql(query_sql)
    df.writeTo(target_table).using("iceberg").append()
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())