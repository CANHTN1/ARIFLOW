import sys
import argparse
from pyspark.sql import SparkSession
from utils.config_loader import load_config_pipeline

def run_data_for_1_report_dt(cob_dt, report_dt, checkpoint=0):
    """Run tasks for a specific report date based on checkpoint."""
    tasks = [
        "insert_dim_salary_empl",
        "insert_dim_salary_cwr",
        "insert_dim_ps_job_dsnv_daily_emp",
        "insert_temp_last_action_cwr_quit_daily",
        "insert_dim_ps_job_dsnv_daily_cwr",
        "insert_dim_prior_exp",
        "insert_dim_review_rating_yr_empl",
        "insert_temp_empl_daily",
        "insert_dim_empl_category_daily",
        "insert_temp_cwr_last_action_daily",
        "insert_dim_cwr_category_daily",
        "insert_dim_dsnv_category_personal_info_daily",
        "insert_dim_dsnv_category_dept_personal_info_daily",
        "insert_dim_dsnv_category_dept_personal_info_all",
        "insert_dim_dsnv_category_dept_no_personal_info_daily",
        "insert_fact_workforce_avg_empl_temp",
        "insert_fact_workforce_avg_empl",
        "insert_fact_ter_empl_monthly_temp",
        "insert_fact_ret_empl_monthly_temp",
        "insert_fact_sth_empl_monthly_temp",
        "insert_fact_new_hire_empl_monthly_temp",
        "insert_fact_turnover_net_increase_empl_temp",
        "insert_fact_turnover_net_increase_empl",
        "insert_fact_wfp_aw_dept_monthly",
    ]

    for i, task_name in enumerate(tasks):
        if checkpoint <= i:
            run_task(task_name, cob_dt, report_dt, checkpoint)

def run_task(task_name, cob_dt, report_dt, checkpoint=0):
    """Run a specific task based on the task name."""
    try:
        sql_cmd = job_config["tables"][task_name]["query"]
        target_table = job_config["tables"][task_name]["target_table"]
        save_mode = job_config["tables"][task_name]["save_mode"]
        task_check_point = int(job_config["tables"][task_name]["check_point"])

        if checkpoint <= task_check_point:
            log.info(f"Executing task: {task_name}")
            df_src = spark.sql(sql_cmd)

            log.info(f"Data loaded for {target_table} with report date {report_dt} and data date {cob_dt}")
            df_src.write.mode(save_mode).insertInto(target_table)
            log.info(f"Finished loading table: {target_table}")
        else:
            log.info(f"Skipping task {task_name} due to checkpoint={checkpoint}")
    except Exception as e:
        log.error(f"Error occurred while inserting into {task_name} with report date {report_dt} and data date {cob_dt}: {e}")
        raise e

if __name__ == "__main__":
    # Create SparkSession
    spark = (SparkSession.builder
             .appName("hrdashboard_daily_spksbmt")
             .enableHiveSupport()
             .getOrCreate())
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger("App")
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process some data with PySpark.")
    parser.add_argument("--cob_dt", required=True, type=str, help="COB Date")
    parser.add_argument("--report_dt", type=str, help="Report Date")
    parser.add_argument("--check_point", type=int, default=0, help="Checkpoint for rerun")
    args = parser.parse_args()

    # Set parameters
    par_cob_dt = args.cob_dt
    par_report_dt = args.report_dt if args.report_dt else par_cob_dt
    par_check_point = args.check_point

    # Load job configuration
    job_config = load_config_pipeline("hrdashboard_daily_spksbmt_config.yml", context_vars={"cob_dt": par_cob_dt, "report_dt": par_report_dt})

    # Process data
    run_data_for_1_report_dt(par_cob_dt, par_report_dt, par_check_point)