import re
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from utils.config_loader import load_config_pipeline

def execute(spark: SparkSession, cob_dt: str, cfg_file: str):
    configs = load_config_pipeline(cfg_file, context_vars={"cob_dt": cob_dt})

    source_df = spark.sql(configs["query"]["select_sql"])

    # Treat empty strings as NULL
    source_df = source_df.na.replace("", None)

    # Validate columns that should not be NULL
    cols_not_null = [col["name"] for col in configs.get("columns") if col.get("not_null")]
    null_expr = " OR ".join(f"{col} IS NULL" for col in cols_not_null)
    invalid_rows = source_df.filter(expr(null_expr))

    if invalid_rows.limit(1).count() > 0:
        invalid_rows.show(truncate=False)
        raise ValueError(f"Null found in required columns: {cols_not_null}")

    source_df.createOrReplaceTempView("source_view")

    primary_key = configs["target"]["native_key"]
    target_table = configs["target"]["table"]

    merge_stmt = f"""
        MERGE INTO {target_table} AS target
        USING source_view AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """ 

    spark.sql(merge_stmt)

def _parse_arguments(args=None):
    parser = argparse.ArgumentParser(description='Spark job for SCD1 transaction')
    parser.add_argument(
        "--cob_dt",
        required=True,
        type=lambda s: s if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s)
        else (_ for _ in ()).throw(argparse.ArgumentTypeError("cob_dt expects YYYY-MM-DD"))
    )
    parser.add_argument('--cfg_file', type=str, required=True)

    return parser.parse_args(args=args)

def main(arguments):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    try:
        execute(spark, arguments.cob_dt, arguments.cfg_file)
    finally:
        spark.stop()

if __name__ == "__main__":
    main(arguments=_parse_arguments())