from airflow import DAG    #Import lớp DAG là thành phần cốt lõi để định nghĩa một quy trình công việc (workflow) trong Airflow.
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator  #dùng để gửi một công việc (job) đến một cụm Apache Spark. Đây là "người thực thi" chính cho việc xử lý dữ liệu.
from airflow.hooks.base import BaseHook  #Lớp cơ sở để tạo các "hook" - cách Airflow tương tác với các dịch vụ bên ngoài. Ở đây nó được dùng để lấy thông tin kết nối.
from airflow.models import Variable #Dùng để truy cập các biến được lưu trữ trong Airflow (Variables), ví dụ như đường dẫn, cấu hình chung.
from airflow.utils.task_group import TaskGroup #Dùng để nhóm các tác vụ (tasks) liên quan lại với nhau trên giao diện người dùng của Airflow, giúp DAG trông gọn gàng hơn.
from datetime import datetime, timedelta #Các module chuẩn của Python để làm việc với ngày và giờ.
import os #Module chuẩn của Python để tương tác với hệ điều hành, ở đây là để liệt kê các file trong thư mục.
from airflow.sensors.sql import SqlSensor #Một loại "cảm biến" đặc biệt, nó sẽ tạm dừng và chờ cho đến khi một câu lệnh SQL trả về kết quả. Dùng để kiểm tra điều kiện từ một CSDL.
from curated.utils.config import load_config, jdbc_connection, insert_process_log, mark_success_log  #Import các hàm tiện ích tự định nghĩa.
from airflow.operators.python import PythonOperator #Operator cho phép thực thi một hàm Python bất kỳ như một tác vụ trong Airflow.

default_args = {
    "owner": "hd.huy", #Tên người sở hữu DAG.
    "concurrency": 1, #Giới hạn số lượng tác vụ của DAG này có thể chạy đồng thời là 1.
    "retries": 1,  #Nếu một tác vụ thất bại, nó sẽ được thử lại 1 lần.
    "retry_delay": timedelta(minutes=15),  #Thời gian chờ giữa các lần thử lại là 15 phút
    "email": ["hd.huy@vietinbank.vn", "stgadmin@vietinbank.vn"], #Cấu hình để gửi email thông báo đến các địa chỉ được liệt kê khi có tác vụ thất bại.
    "email_on_failure": True,
    "email_on_retry": False,
}

CONFIG_DIR = f"{Variable.get('CURATED_JOB_DIR')}/src/config/finance/ingest" #Định nghĩa đường dẫn đến thư mục chứa các file cấu hình .yml. Nó lấy giá trị từ một Airflow Variable tên là CURATED_JOB_DIR.
cob_dt = "{{ dag_run.conf.get('cob_dt', (macros.datetime.now() - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}" #Cố gắng lấy giá trị cob_dt từ cấu hình được truyền vào khi chạy DAG thủ công.Nếu không có, nó sẽ lấy giá trị mặc định là ngày hôm qua.
datalakelog_conn = BaseHook.get_connection("jdbc-postgres-datalakelog") #Lấy đối tượng kết nối có tên jdbc-postgres-datalakelog đã được định nghĩa trong mục Connections của Airflow UI. Đối tượng này chứa các thông tin như host, user, password để kết nối CSDL ghi log.

spark_conf = {
    'spark.executor.cores': '2',
    'spark.executor.instances': '2',
    'spark.executor.memory': '3G',
    'spark.driver.memory': '2G',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.task.maxFailures': '5',
    'spark.yarn.queue': 'default',
    'spark.yarn.appMasterEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
    'spark.yarn.executorEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
}

with DAG(
    dag_id="finance_ingest_curated_dag", #Tên định danh duy nhất cho DAG này.
    default_args=default_args, #Áp dụng các cấu hình mặc định đã khai báo ở trên.
    start_date=datetime(2025, 4, 15), #Ngày đầu tiên mà DAG này có thể được lên lịch chạy.
    catchup=False, #Nếu Airflow bị tắt một thời gian, khi bật lại, nó sẽ không cố gắng chạy lại các lịch trình đã bị bỏ lỡ trong quá khứ.
    schedule_interval='0 9 * * *',  # Lên lịch chạy DAG này hàng ngày vào lúc 9:00 sáng theo cú pháp cron.
    tags=["DPC", "curated", "finance"], #Gán các nhãn cho DAG để dễ dàng tìm kiếm và phân loại trên UI.
    max_active_tasks=10, # Số lượng tác vụ tối đa có thể chạy song song trong DAG này.
    description="Ingest finance tables in groups using SparkSubmit",
) as dag:
    # Dòng này tìm và liệt kê tất cả các file có đuôi .yml hoặc .yaml trong thư mục CONFIG_DIR và sắp xếp chúng theo thứ tự alphabet.
    yml_files = sorted([  
        f for f in os.listdir(CONFIG_DIR)
        if f.endswith(".yml") or f.endswith(".yaml")
    ])

    # Một tác vụ Python dùng để gọi hàm insert_process_log. Mục đích là để ghi lại thông tin vào CSDL log rằng DAG đã bắt đầu chạy.
    start_log_task = PythonOperator(
        task_id="start_log",
        python_callable=insert_process_log,
        op_kwargs={
            "conn_id": "psql_datalake_log",
            "dag_name": dag.dag_id,
            "cob_dt": cob_dt,
        }
    )

    # tác vụ này gọi hàm mark_success_log để cập nhật trạng thái "thành công" vào CSDL log khi toàn bộ DAG hoàn tất.
    end_log_task = PythonOperator(
        task_id="end_log",
        python_callable=mark_success_log,
        op_kwargs={
            "conn_id": "psql_datalake_log",
            "dag_name": dag.dag_id,
            "cob_dt": cob_dt,
        }
    )
    #Bắt đầu một vòng lặp để duyệt qua từng file cấu hình .yml đã tìm thấy.
    task_groups = []
    for yml_file in yml_files:
        config_path = os.path.join(CONFIG_DIR, yml_file)
        group_config = load_config(config_path, context_vars={"cob_dt": cob_dt})

        group_id = os.path.splitext(yml_file)[0]
        with TaskGroup(group_id) as tg:  # Với mỗi file, tạo một TaskGroup mới. Tên của group được lấy từ tên file. Tất cả các tác vụ được định nghĩa bên trong khối with này sẽ được nhóm lại với nhau trên UI.
            for table_name, table_conf in group_config["tables"].items():  #Bên trong mỗi TaskGroup, tiếp tục lặp qua từng "bảng" được định nghĩa trong file cấu hình .yml.
                job_flag = table_conf["flag_job"]
                check_flag_staging = SqlSensor(
                    task_id=f'check_flag_job_{job_flag}',
                    conn_id=table_conf["flag_conn_id"],  #Sử dụng kết nối CSDL được chỉ định để kiểm tra.
                    sql=f"SELECT 1 FROM ETL_PROCESS_LOG WHERE JOB_NAME = UPPER('{job_flag}') AND PROCESS_STS = 'S' AND COB_DT = '{cob_dt}'", #Câu lệnh SQL để kiểm tra xem job ở tầng staging (job_flag) đã chạy thành công (PROCESS_STS = 'S') cho ngày nghiệp vụ (cob_dt) hay chưa.
                    mode='reschedule', #Chế độ hoạt động hiệu quả. Thay vì chiếm một worker slot để chờ, nó sẽ tự giải phóng slot và yêu cầu scheduler kiểm tra lại sau.
                    poke_interval=15 * 60,  # Khoảng thời gian giữa mỗi lần kiểm tra là 15 phút.
                    timeout=12 * 60 * 60,  # Nếu chờ quá 12 giờ mà điều kiện vẫn không đúng, tác vụ sẽ thất bại.

                jdbc_info = jdbc_connection(table_conf["jdbc_conn_id"])
                jdbc_ingestion = SparkSubmitOperator(  #: Tác vụ chính, thực thi việc ingest dữ liệu.
                    task_id=f"ingest_{table_name}",
                    conn_id="spark_default",
                    conf=spark_conf,
                    archives=Variable.get('HDFS_PYSPARK_VENV'),
                    files=Variable.get('HIVE_SITE_FILE'),
                    application=f"{Variable.get('CURATED_JOB_DIR')}/src/pipeline/jdbc_to_iceberg.py", #Chỉ định script Spark (jdbc_to_iceberg.py) sẽ được thực thi.
                    name=f"ingestion_curated_{table_name}",
                    application_args=[                      #Một danh sách các tham số dòng lệnh sẽ được truyền vào script Spark. Các tham số này chứa tất cả thông tin cần thiết để job chạy: thông tin kết nối CSDL nguồn, câu lệnh SQL để lấy dữ liệu, tên bảng Iceberg đích, số lượng phân vùng...
                        "--url",
                        jdbc_info["url"],
                        "--user",
                        jdbc_info["user"],
                        "--password",
                        jdbc_info["password"],
                        "--jdbc_query",
                        table_conf["query"],
                        "--jdbc_driver",
                        jdbc_info["jdbc_driver"],
                        "--metastore_table",
                        table_conf["metastore_table"],
                        "--repartition_num",
                        table_conf["repartition_num"],
                    ],
                )

                metastore_table = table_conf["metastore_table"]
                write_flag_task = SparkSubmitOperator(     #Tác vụ cuối cùng trong chuỗi ingest của một bảng.
                    task_id=f"write_flag_{metastore_table}",
                    conn_id="spark_default",
                    name=f"landing_{metastore_table}_write_log",
                    files=Variable.get('HIVE_SITE_FILE'),
                    application=f"{Variable.get('CURATED_JOB_DIR')}/src/utils/write_flag_table.py", #Chạy một script Spark khác là write_flag_table.py.
                    application_args=[      #Truyền vào các tham số để script này biết cần ghi log cho bảng nào (tbl_name) vào ngày nào (cob_dt). Mục đích là để tạo ra một "cờ" (flag) báo hiệu rằng việc ingest bảng này đã hoàn tất.
                        "--url",
                        datalakelog_conn.host,
                        "--user",
                        datalakelog_conn.login,
                        "--password",
                        datalakelog_conn.password,
                        "--cob_dt",
                        cob_dt,
                        "--tbl_name",
                        metastore_table,
                    ]
                )
                check_flag_staging >> jdbc_ingestion >> write_flag_task
        # Thêm TaskGroup vừa tạo vào một danh sách để thiết lập quan hệ phụ thuộc ở bước cuối cùng.
        task_groups.append(tg)

    # Final flow
    #Dòng này định nghĩa thứ tự thực thi của 3 tác vụ trên: check_flag_staging phải thành công trước thì jdbc_ingestion mới được chạy, và jdbc_ingestion phải thành công thì write_flag_task mới được chạy.
    start_log_task >> task_groups >> end_log_task


