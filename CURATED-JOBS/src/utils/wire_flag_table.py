from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def write_etl_tbl_load_sts(spark, postgres_conn, tbl_name, cob_dt, type_table=None):
    """
    Write load status information to PostgreSQL after a batch job.

    Args:
        spark (SparkSession): The Spark session.
        postgres_conn (dict): PostgreSQL connection information.
        tbl_name (str): The name of the table to flag.
        cob_dt (str): The date partition in format yyyy-MM-dd.
        type_table (str, optional): Type of the table (e.g., "DIM").
    """
    flag_table = "datalakelogdb.etl_tbl_load_sts"

    # Define schema with all columns as String
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
        if type_table and type_table.upper() == "DIM":
            record_count = spark.sql(f"SELECT COUNT(*) FROM {tbl_name}").collect()[0][0]
        else:
            # Read by partition
            record_count = spark.sql(f"SELECT COUNT(*) FROM {tbl_name} WHERE cob_dt = '{cob_dt}'").collect()[0][0]

        # Create a DataFrame for the flag information
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

        logging.info(f"Successfully wrote flag for table {tbl_name} for partition {cob_dt}")

    except Exception as e:
        logging.error(f"Error writing flag: {str(e)}")
        raise

def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description="Process some data with PySpark.")
    parser.add_argument("--url", required=True, help="JDBC connection URL")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--type_table", required=False, help="DIM to disable using COB_DT column in query")
    parser.add_argument("--cob_dt", required=True, help="COB date")
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

    # Create SparkSession
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    try:
        # Run the job
        write_etl_tbl_load_sts(spark, postgres_conn, args.tbl_name, args.cob_dt, args.type_table)
    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    main()