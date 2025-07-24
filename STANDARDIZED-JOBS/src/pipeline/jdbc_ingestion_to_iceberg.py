import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType, IntegerType

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
    """Extract data from JDBC source to Spark and write to Iceberg."""
    
    # Prepare the query
    query = f"({jdbc_query}) AS tmp"

    # Create DataFrame reader
    reader = (
        spark_session.read
        .format("jdbc")
        .option('url', url)
        .option('dbtable', query)
        .option('user', user)
        .option('password', password)
        .option('driver', jdbc_driver)
    )

    # Fetch min and max for partitioning if partition_column is provided
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
        min_val_type = schema["min_val"].dataType
        max_val_type = schema["max_val"].dataType

        # Cast min and max values to IntegerType if they are not Date/Time types
        if not isinstance(min_val_type, (DateType, TimestampType)):
            bounds_df = bounds_df.withColumn("min_val", bounds_df["min_val"].cast(IntegerType()))
        if not isinstance(max_val_type, (DateType, TimestampType)):
            bounds_df = bounds_df.withColumn("max_val", bounds_df["max_val"].cast(IntegerType()))

        bounds_row = bounds_df.collect()[0]
        lower_bound, upper_bound = bounds_row["min_val"], bounds_row["max_val"]

        # Add partition options to the reader
        reader = (
            reader.option("partitionColumn", partition_column)
            .option("lowerBound", lower_bound)
            .option("upperBound", upper_bound)
        )

    # Set additional read options
    if fetch_size:
        reader = reader.option('fetchsize', fetch_size)
    if num_partitions:
        reader = reader.option('numPartitions', num_partitions)

    # Load data and write to Iceberg table
    if spark_session.catalog.tableExists(metastore_table):
        df = reader.load()
        df.repartition(repartition_num).writeTo(metastore_table).overwritePartitions()
    else:
        raise Exception("Error: Table does not exist in Metastore.")

def _parse_arguments(args=None):
    parser = argparse.ArgumentParser(description='Spark-JDBC ingestion to Iceberg')
    parser.add_argument('--url', type=str, required=True, help='JDBC URL')
    parser.add_argument('--user', type=str, required=True, help='Database user')
    parser.add_argument('--password', type=str, required=True, help='Database password')
    parser.add_argument('--jdbc_query', type=str, required=True, help='JDBC query to execute')
    parser.add_argument('--jdbc_driver', type=str, required=True, help='JDBC driver class')
    parser.add_argument('--fetch_size', type=int, default=10000, help='Fetch size for JDBC')
    parser.add_argument('--num_partitions', type=int, help='Number of partitions for JDBC read')
    parser.add_argument('--partition_column', type=str, help='Column to partition the data by')
    parser.add_argument('--metastore_table', type=str, required=True, help='Target Iceberg metastore table')
    parser.add_argument('--repartition_num', type=int, default=1, help='Number of partitions for writing data')

    return parser.parse_args(args=args)

def main(arguments):
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
    finally:
        spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())