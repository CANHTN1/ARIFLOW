import sys
import yaml
import pyspark
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import json
import subprocess
import os
from datetime import datetime
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta
##su - datalake -c "spark-submit --master yarn --deploy-mode cluster --num-executors 5 --executor-cores 3 --driver-memory=5g --executor-memory=5g /software/dags/dags/testing/bid_new.py PROD 2023-07-31"

if __name__ == "__main__":
    # Create SparkSession
    spark = (SparkSession.builder
            .appName("xnk_bcl")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .enableHiveSupport()
            .getOrCreate()
            )

    #   Define current_date
    current_date = datetime.now()
    months_ago = (current_date - relativedelta(months=1)).strftime('%m')
    monthly = current_date.strftime('%m')
    yearly = current_date.strftime('%Y')  
    last_day_of_month = F.last_day(F.lit(current_date))

    # Backdate
    # path = f"hdfs://dr-bigdata-storage.icbv.com:8020/datalake/landing/scrapy/xnk/m{months_ago}.{yearly}_raw_xnk.xlsx"

    path = f"hdfs://dr-bigdata-storage.icbv.com:8020/datalake/landing/scrapy/xnk/m{monthly}.{yearly}_raw_xnk.xlsx"
    print("Path: ", path)

### XU LY DU LIEU XUAT KHAU
# try:
# df_xk = spark.read \
#     .format("com.crealytics.spark.excel") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("dataAddress" , "'XK'!A1") \
#     .load(path)
#     print("Data loaded from Excel successfully.")
# except Exception as e:
#     print("Error loading Excel file:", e)

# pd_xk = df_xk.toPandas()

# df_xk = pd.read_excel(path, sheet_name='XK')

# df_xk = df_xk \
#     .withColumnRenamed("Năm", "nam") \
#     .withColumnRenamed("Tháng", "thang") \
#     .withColumnRenamed("MST", "ma_so_thue") \
#     .withColumnRenamed("Tên DN", "ten_dn") \
#     .withColumnRenamed("Địa chỉ", "dia_chi") \
#     .withColumnRenamed("Tỉnh", "tinh") \
#     .withColumnRenamed("Điện thoại", "dien_thoai") \
#     .withColumnRenamed("Mã nước", "ma_nuoc") \
#     .withColumnRenamed("Tên nước", "ten_nuoc") \
#     .withColumnRenamed(" Kim ngạch", "kim_ngach") \
#     .withColumnRenamed("Mặt hàng lớn nhất", "mat_hang_lon_nhat") \
#     .withColumn("type", F.lit("Xuat khau")) \
#     .withColumn("cob_dt", F.concat_ws("-", F.lit(current_date.strftime('%Y')), F.lit(two_months_ago.strftime('%m')), last_day_of_month))

    df_xk = pd.read_excel(path, sheet_name='XK')

    df_xk = df_xk.rename(columns={
        "Năm": "nam",
        "Tháng": "thang",
        "MST": "ma_so_thue",
        "Tên DN": "ten_dn",
        "Địa chỉ": "dia_chi",
        "Tỉnh": "tinh",
        "Điện thoại": "dien_thoai",
        "Mã nước": "ma_nuoc",
        "Tên nước": "ten_nuoc",
        " Kim ngạch": "kim_ngach",
        "Mặt hàng lớn nhất": "mat_hang_lon_nhat"
    })

    df_xk["type"] = "Xuat khau"
    df_xk["cob_dt"] = "{}-{:02d}-{}".format(current_date.year, current_date.month, last_day_of_month)

#Backdate
# df_xk["cob_dt"] = "2025-05-31"

# them cot partition t1-2023
# pd_xk['type'] = 'Xuat khau'
# pd_xk['cob_dt'] = '2024-09-30' 

# convert pandas to dataframe 
    pd_xk_df = spark.createDataFrame(df_xk)

# tao bang temp xuat khau thang 11 2023
    pd_xk_df.createOrReplaceTempView("tmp_xk")   

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    spark.sql("""
        INSERT INTO outsite.xnk_bcl
            SELECTT
            nam,
            thang,
            ma_so_thue,
            ten_dn,
            dia_chi,
            tinh,
            dien_thoai,
            ma_nuoc,
            ten_nuoc,
            kim_ngach,
            mat_hang_lon_nhat,
            cob_dt,
            type
        FROM tmp_xk
    """)    

############################################################################################
### XU LY DU LIEU NHAP KHAU
# df_nk= spark.read \
#     .format("com.crealytics.spark.excel") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("dataAddress" , "'NK'!A1") \
#     .load(path)

    df_nk = pd.read_excel(path, sheet_name='NK')

# pd_nk = df_nk.toPandas()

    df_nk.rename(columns = {'Năm':'nam', 
        'Tháng':'thang', 
        'MST':'ma_so_thue', 
        'Tên DN':'ten_dn', 
        'Địa chỉ':'dia_chi', 
        'Tỉnh':'tinh', 
        'Điện thoại':'dien_thoai',
        'Mã nước':'ma_nuoc', 
        'Tên nước':'ten_nuoc', 
        ' Kim ngạch':'kim_ngach', 
        'Mặt hàng lớn nhất':'mat_hang_lon_nhat'}, inplace=True)

# them cot partition t1-2023
    df_nk['type'] = 'Nhap khau'
    df_nk["cob_dt"] = "{}-{:02d}-{}".format(current_date.year, current_date.month, last_day_of_month)

#Backdate
# df_nk["cob_dt"] = "2025-05-31"

# convert pandas to dataframe 
    pd_nk_df = spark.createDataFrame(df_nk)

# tao bang temp xuat khau thang 11 2023
    pd_nk_df.createOrReplaceTempView("tmp_nk")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    spark.sql("""
        INSERT INTO outsite.xnk_bcl
            SELECT
            nam,
            thang,
            ma_so_thue,
            ten_dn,
            dia_chi,
            tinh,
            dien_thoai,
            ma_nuoc,
            ten_nuoc,
            kim_ngach,
            mat_hang_lon_nhat,
            cob_dt,
            type
        FROM tmp_nk
    """)    