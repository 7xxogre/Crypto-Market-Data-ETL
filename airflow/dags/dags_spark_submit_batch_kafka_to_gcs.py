from airflow import DAG
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'start_date': pendulum.datetime(2024, 3, 21),
    'depends_on_past': False,
}

dag = DAG("dags_spark_submit_batch_kafka_to_gcs", 
          default_args=default_args,
          schedule_interval="0 0 * * *",
          catchup=False)

spark_job = SparkSubmitOperator(
    task_id="spark_batch_operator_for_kafka_to_gcs",
    application="/home/bestech49/airflow-docker/plugins/spark_funcs/kafka_to_gcs_by_spark_batch.py",
    application_args=[
        '--execution-date', "{{ ds }}",
        '--kafka-bootstrap-server-list-name', 'kafka_broker_ips.txt',
        '--topic-name', 'upbit_orderbook',
        '--num_partitions', '0',
        '--gcs-name', 'crypto-market-data-gcs',
        '--gcs-save-path', 'upbit/orderbook',
        '--app-name', 'upbit-orderbook-save-to-gcs'
    ],
    conn_id='spark_default',
    dag=dag,
)