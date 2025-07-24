import argparse
from pyspark.sql import SparkSession

def ingestion_hive_to_hive(spark_session, query, metastore_table, repartition_num):
    # Execute the SQL query to get the DataFrame
    df = spark_session.sql(query)

    # Repartition and write the DataFrame to the specified metastore table
    df.repartition(int(repartition_num)).writeTo(metastore_table).using("iceberg").overwritePartitions()
    return "Done"

def _parse_arguments(args=None):
    parser = argparse.ArgumentParser(description='Spark-SQL ingestion job')
    parser.add_argument('--query', type=str, required=True, help='SQL query to execute')
    parser.add_argument('--metastore_table', type=str, required=True, help='Target metastore table')
    parser.add_argument('--repartition_num', type=str, required=True, help='Number of partitions for the output')

    return parser.parse_args(args=args)

def main(arguments):
    # Initialize Spark session with Hive support
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Set configuration for partition overwrite mode
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    # Ingest data from Hive to Hive
    result = ingestion_hive_to_hive(
        spark,
        arguments.query,
        arguments.metastore_table,
        arguments.repartition_num
    )
    
    print(result)  # Print the result message
    spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())