from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from kafka import KafkaConsumer, TopicPartition



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dags_spark_submit_bash_process_raw_data_from_gcs',
    default_args=default_args,
    description='raw data preprocessing from GCS by Pyspark',
    schedule_interval='0 0 * * *',
    catchup=False
)

gcs_name = ""
dataproc_cluster_name = ""
region = "asia-northeast3"
spark_submit_command = f"""
gcloud dataproc jobs submit pyspark \
    gs://{gcs_name}/processing_raw_data_from_gcs.py \
    --cluster={dataproc_cluster_name} \
    --region={region} \
    --properties  spark.dynamicAllocation.enabled=true,spark.shuffle.service.enabled=true,spark.dynamicAllocation.initialExecutors=1,spark.dynamicAllocation.minExecutors=1,spark.dynamicAllocation.maxExecutors=3 \
    -- \
    --kafka-bootstrap-server-list-file-name 'kafka_broker_ips.txt' \
    --gcs-name '{gcs_name}'""" + """\
    --execution-date "{{ execution_date.strftime('%Y-%m-%d') }}" \
    --gcs-save-path 'upbit/orderbook' \
    --dollar-bar-size 3000000 \
    --app-name 'upbit-data-preprocessing-from-gcs-to-gcs'
    """


data_preprocessing_job_pyspark = BashOperator(
    task_id='data_preprocessing_job_pyspark',
    bash_command=spark_submit_command,
    dag=dag,
)