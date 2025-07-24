from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, minute
)
import argparse

# Argument parsing
parser = argparse.ArgumentParser(description="Process some data with PySpark.")
parser.add_argument("--cob_dt", required=True, help="Cob date")
args = parser.parse_args()
cob_dt = args.cob_dt

# Create Spark session
spark = SparkSession.builder \
    .appName("Qlrrhd twr") \
    .enableHiveSupport() \
    .getOrCreate()

# Query to get transaction data
qlrrhd_df = spark.sql(f"""
    SELECT CAST(t.cob_dt AS DATE) AS cob_dt, hh, mi, twr_type, status, COUNT(1) AS sl 
    FROM (
        SELECT cob_dt,
            HOUR(CAST(time AS TIMESTAMP)) AS hh,
            MINUTE(CAST(time AS TIMESTAMP)) AS mi,
            id AS tran_id,
            'WITHDRAW_ATM' AS twr_type,
            CASE 
                WHEN respcode = '1' THEN 'S'
                WHEN respcode != '1' AND host IN ('100', '234', '232', '235') THEN 'F_VTB'
                ELSE 'F_PARTNER'
            END AS status
        FROM twr.twr_tla_view_daily 
        WHERE cob_dt = '{cob_dt}' AND trancode = '10'

        UNION 

        SELECT cob_dt,
            HOUR(CAST(time AS TIMESTAMP)) AS hh,
            MINUTE(CAST(time AS TIMESTAMP)) AS mi,
            id AS tran_id,
            'WITHDRAW_ATM_QR' AS twr_type,
            CASE 
                WHEN respcode = '1' THEN 'S'
                WHEN respcode != '1' AND host IN ('100', '234', '232', '235') THEN 'F_VTB'
                ELSE 'F_PARTNER'
            END AS status
        FROM twr.twr_tla_view_daily 
        WHERE cob_dt = '{cob_dt}' AND trancode = '10' AND toacct = 'QRCODEQR1QR2'

        UNION 

        SELECT cob_dt,
            HOUR(CAST(time AS TIMESTAMP)) AS hh,
            MINUTE(CAST(time AS TIMESTAMP)) AS mi,
            id AS tran_id,
            'PAYMENT_POS' AS twr_type,
            CASE 
                WHEN respcode = '1' THEN 'S'
                WHEN respcode != '1' AND host IN ('100', '234', '232', '235') THEN 'F_VTB'
                ELSE 'F_PARTNER'
            END AS status
        FROM twr.twr_tla_view_daily 
        WHERE cob_dt = '{cob_dt}' AND trancode = '110' AND poscondition NOT IN ('52','53','59','61','62','65','66','81','82','83','84','85','86','87','88','89')

        UNION 

        SELECT cob_dt,
            HOUR(CAST(time AS TIMESTAMP)) AS hh,
            MINUTE(CAST(time AS TIMESTAMP)) AS mi,
            id AS tran_id,
            'PAYMENT_ONLINE' AS twr_type,
            CASE 
                WHEN respcode = '1' THEN 'S'
                WHEN respcode != '1' AND host IN ('100', '234', '232', '235') THEN 'F_VTB'
                ELSE 'F_PARTNER'
            END AS status
        FROM twr.twr_tla_view_daily 
        WHERE cob_dt = '{cob_dt}' AND trancode = '110' AND poscondition IN ('52','53','59','61','62','65','66','81','82','83','84','85','86','87','88','89')
    ) t
    WHERE status IS NOT NULL
    GROUP BY cob_dt, hh, mi, twr_type, status
""")

# Show the DataFrame
qlrrhd_df.show()

# Write DataFrame to Hive table
qlrrhd_df.write.mode("overwrite").insertInto("hive.cur_kvh.qlrrhd_twr")

# Stop the Spark session
spark.stop()