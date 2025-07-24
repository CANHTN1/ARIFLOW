from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType
import argparse

def write_etl_tbl_load_sts(spark, postgres_conn,tbl_name, cob_dt):
    """
    Ghi thông tin cờ vào PostgreSQL sau khi thực hiện batch job thành công, theo partition.

    :param spark: SparkSession.
    :param postgres_conn: Thông tin kết nối PostgreSQL.
    :param tbl_name: Tên bảng cần đánh cờ.
    :param cob_dt: Ngày partition cần đọc, định dạng yyyy-MM-dd.
    """
    flag_table = "datalakelogdb.etl_tbl_load_sts"

        # Xác định schema với tất cả các cột là kiểu String
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
        # Đọc theo partition
        record_count = spark.sql(f"SELECT COUNT(*) FROM {tbl_name} WHERE cob_dt = '{cob_dt}'").collect()[0][0]
    
        # Tạo DataFrame chứa thông tin flag
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

        # Ghi vào PostgreSQL
        flag_df.write \
            .format("jdbc") \
            .options(**postgres_conn) \
            .option("dbtable", flag_table) \
            .mode("append") \
            .save()

        print(f"Flag ghi thành công bảng {tbl_name} cho partition {cob_dt}")

    except Exception as e:
        print(f"Lỗi ghi flag: {str(e)}")
        raise
def main():
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

    # Tạo SparkSession
    spark = SparkSession.builder.enableHiveSupport().getOrCreate() 

    # Chạy job
    write_etl_tbl_load_sts(spark, postgres_conn, args.tbl_name, args.cob_dt)

    # Dừng SparkSession
    spark.stop()
if __name__ == "__main__":
    main()