import argparse
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)

def ingestion_hive_to_hive(
        spark_session,
        query,
        metastore_table,
        save_mode,
        save_format
):
    """
    Ingest data from one Hive table to another using a SQL query.

    Args:
        spark_session (SparkSession): The Spark session.
        query (str): The SQL query to execute.
        metastore_table (str): The target metastore table name.
        save_mode (str): The save mode (e.g., 'append', 'overwrite').
        save_format (str): The format in which to save the data (e.g., 'parquet').

    Returns:
        str: A confirmation message upon completion.
    """
    try:
        df = spark_session.sql(query)
        df.write.mode(save_mode) \
            .format(save_format) \
            .option("partitionOverwriteMode", "dynamic") \
            .insertInto(metastore_table)
        logging.info("Data ingestion completed successfully.")
        return "Done"
    except Exception as e:
        logging.error(f"Error during ingestion: {str(e)}")
        raise

def _parse_arguments(args=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Spark-SQL')
    parser.add_argument('--query', type=str, required=True, help='SQL query to execute')
    parser.add_argument('--metastore_table', type=str, required=True, help='Target metastore table name')
    parser.add_argument('--save_mode', type=str, required=True, help='Save mode for the write operation')
    parser.add_argument('--save_format', type=str, required=True, help='Format for the write operation')

    return parser.parse_args(args=args)

def main(arguments):
    """Main entry point for the script."""
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    try:
        ingestion_hive_to_hive(
            spark,
            arguments.query,
            arguments.metastore_table,
            arguments.save_mode,
            arguments.save_format
        )
    except Exception as e:
        logging.error(f"Execution failed: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())