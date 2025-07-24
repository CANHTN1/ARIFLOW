import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType, IntegerType
from pyspark.sql.functions import col, lit, trim

# Configure logging
logging.basicConfig(level=logging.INFO)

def ingestion_jdbc_to_iceberg(
        spark_session,
        url,
        user,
        password,
        jdbc_query,
        jdbc_driver,
        fetch_size,
        num_partitions,
        partition_column,
        metastore_table,
        repartition_num
):
    """Extract data from JDBC source to Iceberg table."""
    query = f"({jdbc_query}) tmp"

    reader = (
        spark_session.read
        .format("jdbc")
        .option('url', url)
        .option('dbtable', query)
        .option('user', user)
        .option('password', password)
        .option('driver', jdbc_driver)
    )

    # Calculate lowerBound and upperBound using a separate query
    if partition_column:
        min_max_query = f"(SELECT MIN({partition_column}) AS min_val, MAX({partition_column}) AS max_val FROM ({jdbc_query}) AS subquery) AS bounds"
        bounds_df = (
            spark_session.read
            .format("jdbc")
            .option("url", url)
            .option("dbtable", min_max_query)
            .option("user", user)
            .option("password", password)
            .option("driver", jdbc_driver)
            .load()
        )

        schema = bounds_df.schema
        min_val_type, max_val_type = schema["min_val"].dataType, schema["max_val"].dataType

        if not isinstance(min_val_type, (DateType, TimestampType)):
            bounds_df = bounds_df.withColumn("min_val", bounds_df["min_val"].cast(IntegerType()))
        if not isinstance(max_val_type, (DateType, TimestampType)):
            bounds_df = bounds_df.withColumn("max_val", bounds_df["max_val"].cast(IntegerType()))

        bounds_row = bounds_df.collect()[0]
        lower_bound, upper_bound = bounds_row["min_val"], bounds_row["max_val"]

        reader = (
            reader.option("partitionColumn", partition_column)
            .option("lowerBound", lower_bound)
            .option("upperBound", upper_bound)
        )

    # Set specific read options
    if fetch_size:
        reader = reader.option('fetchsize', fetch_size)
    if num_partitions:
        reader = reader.option('numPartitions', int(num_partitions))

    if spark_session.catalog.tableExists(metastore_table):
        df = reader.load()
        df = df.repartition(repartition_num)
        df.writeTo(metastore_table).overwritePartitions()
        logging.info(f"Data successfully written to {metastore_table}.")
    else:
        logging.error("Error: Table does not exist in Metastore.")
        raise Exception("Error: Table does not exist in Metastore.")

def _parse_arguments(args=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Spark-JDBC to Iceberg')
    parser.add_argument('--url', type=str, required=True)
    parser.add_argument('--user', type=str, required=True)
    parser.add_argument('--password', type=str, required=True)
    parser.add_argument('--jdbc_query', type=str, required=True)
    parser.add_argument('--jdbc_driver', type=str, required=True)
    parser.add_argument('--fetch_size', type=int, default=10000)
    parser.add_argument('--num_partitions', type=int)
    parser.add_argument('--partition_column', type=str)
    parser.add_argument('--metastore_table', type=str, required=True)
    parser.add_argument('--repartition_num', type=int, default=1)

    return parser.parse_args(args=args)

def main(arguments):
    """Main entry point for the script."""
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    try:
        ingestion_jdbc_to_iceberg(
            spark,
            arguments.url,
            arguments.user,
            arguments.password,
            arguments.jdbc_query,
            arguments.jdbc_driver,
            arguments.fetch_size,
            arguments.num_partitions,
            arguments.partition_column,
            arguments.metastore_table,
            arguments.repartition_num
        )
    except Exception as e:
        logging.error(f"Error during ingestion: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())