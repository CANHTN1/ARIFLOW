from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def copy_parquet_to_iceberg(spark, source_path, target_table):
    """
    Copy data from a Parquet file to an Iceberg table.

    Args:
        spark (SparkSession): The Spark session.
        source_path (str): Path to the source Parquet files.
        target_table (str): The target Iceberg table name.
    """
    try:
        logging.info(f"Starting copy from {source_path} to {target_table}...")
        df_src = spark.read.format("parquet").load(source_path)
        df_src.write.format("iceberg").mode("overwrite").save(target_table)
        logging.info(f"Successfully copied to {target_table}.")
    except Exception as e:
        logging.error(f"Error while copying to {target_table}: {str(e)}")
        raise

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # List of source and target pairs
    copy_tasks = [
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/fact_ter_empl_monthly_temp/", 'hive.dtm_hr360.fact_ter_empl_monthly_temp'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/fact_turnover_net_increase_empl/", 'hive.dtm_hr360.fact_turnover_net_increase_empl'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/fact_turnover_net_increase_empl_temp/", 'hive.dtm_hr360.fact_turnover_net_increase_empl_temp'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/fact_wfp_aw_dept_monthly/", 'hive.dtm_hr360.fact_wfp_aw_dept_monthly'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/fact_workforce_avg_empl/", 'hive.dtm_hr360.fact_workforce_avg_empl'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/fact_workforce_avg_empl_temp/", 'hive.dtm_hr360.fact_workforce_avg_empl_temp'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/temp_cwr_last_action_daily/", 'hive.dtm_hr360.temp_cwr_last_action_daily'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/temp_empl_daily/", 'hive.dtm_hr360.temp_empl_daily'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/temp_last_action_cwr_quit_daily/", 'hive.dtm_hr360.temp_last_action_cwr_quit_daily'),
        ("hdfs://dr-bigdata-storage.icbv.com:8020/datalake/sandbox/hr360_old/test_load_spark_job/", 'hive.dtm_hr360.test_load_spark_job'),
    ]

    # Run copy operations
    for source_path, target_table in copy_tasks:
        copy_parquet_to_iceberg(spark, source_path, target_table)

    # Stop SparkSession
    spark.stop()