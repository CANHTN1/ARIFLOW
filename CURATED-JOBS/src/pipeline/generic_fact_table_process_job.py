import argparse
import re
import logging
from pyspark.sql import SparkSession
from utils.config_loader import load_config_pipeline

# Configure logging
logging.basicConfig(level=logging.INFO)

def _parse_arguments(args=None):
    """
    Parse command-line arguments.

    Args:
        args (list, optional): List of arguments to parse.

    Returns:
        Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description='Spark SCD2 Job')
    parser.add_argument(
        "--cob_dt",
        required=True,
        type=lambda s: s if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s)
        else (_ for _ in ()).throw(argparse.ArgumentTypeError("cob_dt expects YYYY-MM-DD"))
    )
    parser.add_argument('--cfg_file', type=str, required=True)
    return parser.parse_args(args=args)

def run_staging_process(spark, stages):
    """
    Execute the staging processes.

    Args:
        spark (SparkSession): The Spark session.
        stages (list): List of staging configurations.
    """
    for stage in stages:
        staging_name = stage["name"]
        staging_presql = stage.get("pre_sql", "")
        staging_sql = stage.get("select_sql", "")
        staging_table_name = stage.get("table_name", "")

        logging.info(f"Starting process for staging: {staging_name}")
        logging.info(f"Running staging pre-query: {staging_presql}")
        spark.sql(staging_presql)

        logging.info(f"Running staging main query: {staging_sql}")
        df_staging = spark.sql(staging_sql)
        df_staging.writeTo(staging_table_name).using("iceberg").overwritePartitions()

def main(arguments):
    cob_dt = arguments.cob_dt
    yml_file = arguments.cfg_file

    # Load config
    job_config = load_config_pipeline(yml_file, context_vars={"cob_dt": cob_dt})
    job_name = job_config["spark"]["job_name"]
    target_table = job_config["target"]["table_name"]
    preload = job_config["target"].get("preload", 0)
    query_sql = job_config["target"]["query_sql"]
    write_mode = job_config["target"].get("write_mode", "append")

    # Create Spark session
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    try:
        # Optional preload
        if preload == 1:
            preload_sql = job_config["target"]["preload_sql"]
            logging.info(f"Running preload SQL: {preload_sql}")
            spark.sql(preload_sql)

        # Run staging processes
        staging_stages_cnt = job_config["staging"].get("total_stage", 0)
        if staging_stages_cnt > 0:
            run_staging_process(spark, job_config["staging"]["stages"])
        else:
            logging.info("No staging stages to process.")

        # Run main query
        logging.info(f"Executing main query: {query_sql}")
        df = spark.sql(query_sql)

        # Write to Iceberg table
        if write_mode == "overwrite":
            logging.info(f"Overwriting partition in table: {target_table}")
            df.writeTo(target_table).using("iceberg").overwritePartitions()
        else:
            logging.info(f"Appending to table: {target_table}")
            df.writeTo(target_table).using("iceberg").append()

    except Exception as e:
        logging.error(f"Error during processing: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())