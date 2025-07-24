import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, trim
from pyspark.sql.types import DateType, TimestampType, IntegerType

def ingestion_jdbc_to_hive(
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
    repartition_num,
    save_mode,
    save_format
):
    """Extract data from JDBC source to Spark"""
    query = f"({jdbc_query}) tmp"

    df = (
        spark_session.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", query)
        .option("user", user)
        .option("password", password)
        .option("driver", jdbc_driver)
        .load()
    )

    df.printSchema()
    df.show()
def _parse_arguments(args=None):
    parser = argparse.ArgumentParser(description="Spark-JDBC")
    parser.add_argument("--url", type=str, required=True)
    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)
    parser.add_argument("--jdbc_query", type=str, required=True)
    parser.add_argument("--jdbc_driver", type=str, required=True)
    parser.add_argument("--fetch_size", type=int, default=10000)
    parser.add_argument("--num_partitions", type=int)
    parser.add_argument("--partition_column", type=str)
    parser.add_argument("--metastore_table", type=str, required=True)
    parser.add_argument("--repartition_num", type=int, default=1)
    parser.add_argument("--save_mode", type=str, required=True)
    parser.add_argument("--save_format", type=str, required=True)


    return parser.parse_args(args=args)
def main(arguments):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    ingestion_jdbc_to_hive(
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
        arguments.repartition_num,
        arguments.save_mode,
        arguments.save_format
    )
if __name__ == "__main__":
    main(arguments=_parse_arguments())