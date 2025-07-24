import argparse
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
from utils.config_loader import load_config_pipeline

def _parse_arguments(args=None):
    parser = argparse.ArgumentParser(description='Spark SCD2 Job')
    parser.add_argument(
        "--cob_dt",
        required=True,
        type=lambda s: s if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s)
        else (_ for _ in ()).throw(argparse.ArgumentTypeError("cob_dt expects YYYY-MM-DD"))
    )
    parser.add_argument('--cfg_file', type=str, required=True)
    return parser.parse_args(args=args)

def is_diff(col_name):
    left = col(f"new.{col_name}")
    right = col(f"cur.{col_name}")
    return (left != right) | ((left.isNull() & right.isNotNull()) | (left.isNotNull() & right.isNull()))

def main(arguments):
    cob_dt = arguments.cob_dt
    yml_file = arguments.cfg_file

    # Load configuration
    job_config = load_config_pipeline(yml_file, context_vars={"cob_dt": cob_dt})
    job_name = job_config["spark"]["job_name"]
    
    # Initialize Spark session
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # 1. Prepare staging table
    staging_table = job_config["stagging"]["table_name"]
    staging_sql = job_config["stagging"]["query_sql"]
    staging_preload = job_config["stagging"]["preload_sql"]

    source_cols = [col_cfg["name"] for col_cfg in job_config["columns"]]
    surrogate_key = job_config["target"]["surogate_key"]
    df_staging = spark.sql(staging_sql)

    if staging_table != 'NONE':
        print("Executing preload SQL:", staging_preload)
        spark.sql(staging_preload)

        print("Writing to staging table with SQL:", staging_sql)
        df_staging.writeTo(staging_table).using("iceberg").append()
        df_cur = spark.table(staging_table).select(*source_cols, surrogate_key)
    else:
        df_cur = df_staging.select(*source_cols, surrogate_key)

    # 2. Load new data
    select_sql = job_config["query"]["select_sql"]
    print("Running select SQL:\n", select_sql)
    df_new = spark.sql(select_sql)

    # 3. Detect inserts
    native_keys = [col["name"] for col in job_config["columns"] if col.get("native_key", 0) == 1]
    df_insert = df_new.join(df_cur, on=native_keys, how="left_anti")

    # 4. Detect updates
    df_joined = df_new.alias("new").join(df_cur.alias("cur"), on=native_keys)
    conditions = [is_diff(col_cfg["name"]) for col_cfg in job_config["columns"] if col_cfg.get("do_not_compare", 0) == 0]

    if conditions:
        filter_expr = conditions[0]
        for cond in conditions[1:]:
            filter_expr |= cond

        df_update = df_joined.filter(filter_expr).select(
            "new.*",
            col(f"cur.{surrogate_key}").alias("prev_surogate_key")
        )
    else:
        df_update = spark.createDataFrame([], df_new.schema).withColumn("prev_surogate_key", lit(None).cast(StringType()))

    # 5. Update changed records
    df_update.createOrReplaceTempView("updated_df_view")

    target_table = job_config["target"]["table_name"]
    
    # Ensure insert columns match target table schema
    target_cols = [field.name for field in spark.table(target_table).schema.fields]
    sql_stmt = f"""
    INSERT INTO {target_table}
    SELECT {", ".join(target_cols)}
    FROM updated_df_view
    """
    print(f"Executing insert for updated records: {sql_stmt}")
    spark.sql(sql_stmt)

    # Mark old records as expired
    update_old_record_stmt = f"""
        MERGE INTO {target_table} AS target
        USING updated_df_view AS source
        ON target.{surrogate_key} = source.prev_surogate_key
        WHEN MATCHED THEN UPDATE SET
            target.is_cur = 0,
            target.eff_end_dt = DATE '{cob_dt}'
    """
    print(f"Marking previous versions as not current: {update_old_record_stmt}")
    spark.sql(update_old_record_stmt)

    # Finally, insert new records
    print(f"Inserting new records into target: {target_table}")
    df_insert.writeTo(target_table).using("iceberg").append()

    spark.stop()
    print("SCD2 job completed successfully.")

if __name__ == "__main__":
    main(arguments=_parse_arguments())