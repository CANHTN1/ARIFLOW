from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit
import sys
import argparse

def generate_date_range(start_date_str, end_date_str):

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    # Tạo danh sách các ngày
    date_list = []
    while start_date < end_date:
        date_list.append(start_date.strftime('%Y-%m-%d'))
        start_date += timedelta(days=1)

    return date_list
def insert_df(df, target_table, spark, cob_dt, path_mode):
    lowercase_columns = [column.lower() for column in df.columns]
    df = df.toDF(*lowercase_columns)

    # Align dataframe columns to Hive table schema
    table_schema = spark.table(target_table).schema
    column_order = [column.lower() for column in table_schema.names]

    if path_mode == 'all':
        df = df.withColumn("cob_dt", col("cob_dt").cast("string"))
    elif path_mode in ['cob_dt', 'COB_DT']:
        df = df.withColumn("cob_dt", lit(cob_dt).cast("string"))

    df = df.select(
        *[
            col(column_name).alias(column_name) if column_name in df.columns
            else lit(None).alias(column_name)
            for column_name in column_order
        ]
    )

    ( 
        df.write
        .format("orc")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .insertInto(target_table)
    )

def main(run_mode, start_date, end_date, path_mode, input_path, target_table, hms_table):
    spark = SparkSession.builder \
        .appName("Backdate Hive Overwrite Job") \
        .enableHiveSupport() \
        .getOrCreate()


    if run_mode == '1': # run_mode 1
        if path_mode in ['cob_dt', 'COB_DT']:
            date_range = generate_date_range(start_date, end_date)
        elif path_mode == 'all':
            date_range = [1]
        else:
            raise Exception("Lỗi: path_mode không hợp lệ")
    
        for cob_dt in date_range:
            print(f"Processing date: {cob_dt}")
            if path_mode == 'cob_dt':
                hdfs_path = f"hdfs://dr-bigdata-storage.icbv.com:8020/{input_path}/cob_dt={cob_dt}"
            elif path_mode == 'COB_DT':
                hdfs_path = f"hdfs://dr-bigdata-storage.icbv.com:8020/{input_path}/COB_DT={cob_dt}"
            elif path_mode == 'all':
                hdfs_path = f"hdfs://dr-bigdata-storage.icbv.com:8020/{input_path}"
            else:
                raise Exception("Lỗi: path_mode không hợp lệ")
            
            try:
                df = spark.read.orc(hdfs_path)
            except:
                print(f"error when read {hdfs_path} with path_mode {path_mode}")
                continue

            insert_df(df=df, target_table=target_table, spark=spark, cob_dt=cob_dt, path_mode=path_mode)

    elif run_mode == '2':  # run_mode 2      
        hive_source_table = hms_table if hms_table != "" else ""  # Replace or pass in as argument

        # Get list of partitions
        partitions = spark.sql(f"SHOW PARTITIONS {hive_source_table}").rdd.map(lambda row: row[0]).collect()

        for partition in partitions:
            # Convert 'dt=20250424' to dict { 'dt': '20250424' }
            partition_kv = dict(kv.split('=') for kv in partition.split('/'))

            # Build where clause
            cob_dt = '/'.join([f"{v}" for k, v in partition_kv.items()]) 

            if path_mode == 'cob_dt':
                hdfs_path = f"hdfs://dr-bigdata-storage.icbv.com:8020/{input_path}/cob_dt={cob_dt}"
            elif path_mode == 'COB_DT':
                hdfs_path = f"hdfs://dr-bigdata-storage.icbv.com:8020/{input_path}/COB_DT={cob_dt}"
            elif path_mode == 'all':
                hdfs_path = f"hdfs://dr-bigdata-storage.icbv.com:8020/{input_path}"
            else:
                raise Exception("Lỗi: path_mode không hợp lệ")
            try:
                df = spark.read.orc(hdfs_path)
            except:
                print(f"error when read {hdfs_path} with path_mode {path_mode}")
                continue

            insert_df(df=df, target_table=target_table, spark=spark, cob_dt=cob_dt, path_mode=path_mode)

    else: 
        spark.stop()
        raise Exception("Lỗi: run_mode không hợp lệ")


    spark.stop()
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_mode", required=True, help="1 for date range run; 2 for partition run" )
    parser.add_argument("--path_mode", required=True, help=" cob_dt: input_path with cob_dt; COB_DT: input_path with COB_DT; all: run full table")
    parser.add_argument("--input_path", required=True, help="path of orc hive file")
    parser.add_argument("--target_table", required=True, help="Target Hive table name (e.g., db.trans_tmp)")
    parser.add_argument("--start_date", help="Start date in format yyyy-mm-dd // required on run_mode 1 path_mode != all")
    parser.add_argument("--end_date", help="End date in format yyyy-mm-dd // required on run_mode 1 path_mode != all")
    parser.add_argument("--hms_table", help="source table // required if run_mode 2")


    args = parser.parse_args()

    main(run_mode=args.run_mode, start_date=args.start_date, end_date=args.end_date, path_mode=args.path_mode, input_path=args.input_path, target_table=args.target_table, hms_table=args.hms_table)