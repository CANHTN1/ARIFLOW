from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
import argparse
import logging

def write_etl_tbl_load_sts(spark, postgres_conn, tbl_name, cob_dt):
    """
    Write ETL status information to PostgreSQL after a successful batch job, per partition.

    Args:
        spark (SparkSession): The Spark session.
        postgres_conn (dict): PostgreSQL connection information.
        tbl_name (str): The name of the table to flag.
        cob_dt (str): The partition date to read, formatted as yyyy-MM-dd.
    """
    flag_table = "datalakelogdb.etl_tbl_load_sts"

    # Define schema with all columns as String type
    schema = StructType([
        StructField("TBL_NAME", StringType(), True),
        StructField("COB_DT", StringType(), True),
        StructField("RUN_SEQ", StringType(), True),
        StructField("Start_dtm", StringType(), True),
        StructField("End_dtm", StringType(), True),
        StructField("Updtd_dtm", StringType(), True),
        StructField("PROCESS_STS", StringType(), True),
        StructField("PROCESS_ROW", StringType(), True),
    ])

    try:
        # Read record count
        record_count = spark.sql(f"SELECT COUNT(*) FROM {tbl_name}").collect()[0][0]
        
        # Create DataFrame with flag information
        flag_df = spark.createDataFrame([{
            "TBL_NAME": tbl_name,
            "COB_DT": cob_dt,
            "RUN_SEQ": str(1),
            "Start_dtm": None,
            "End_dtm": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Updtd_dtm": None,
            "PROCESS_STS": "S",
            "PROCESS_ROW": str(record_count)
        }], schema=schema)

        # Write to PostgreSQL
        flag_df.write \
            .format("jdbc") \
            .options(**postgres_conn) \
            .option("dbtable", flag_table) \
            .mode("append") \
            .save()

        logging.info(f"Flag written successfully for table {tbl_name} for partition {cob_dt}")

    except Exception as e:
        logging.error(f"Error writing flag: {str(e)}")
        raise

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Create argument parser
    parser = argparse.ArgumentParser(description="Process some data with PySpark.")
    parser.add_argument("--url", required=True, help="JDBC connection URL")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--cob_dt", required=True, help="Cob date")
    parser.add_argument("--tbl_name", required=True, help="Destination table")

    # Parse arguments
    args = parser.parse_args()

    # Create PostgreSQL connection options
    postgres_conn = {
        "url": args.url,
        "user": args.user,
        "password": args.password,
        "driver": "org.postgresql.Driver",
    }

    # Create Spark session
    spark = SparkSession.builder.enableHiveSupport().getOrCreate() 

    # Run job
    write_etl_tbl_load_sts(spark, postgres_conn, args.tbl_name, args.cob_dt)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()