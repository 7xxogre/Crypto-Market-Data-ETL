from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from kafka import KafkaConsumer, TopicPartition

def download_blob(bucket_name, source_blob_name):
    """GCS에서 파일 내용을 다운로드하고 출력합니다."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    
    # Blob의 내용을 문자열로 다운로드
    contents = blob.download_as_string()
    
    return contents.decode("utf-8").split()

def get_kafka_offset(bootstrap_servers, topic_name, num_partition, execution_date, interval_hours=1):
    """주어진 시간 기반으로 Kafka offset을 조회합니다."""
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000
    )

    prev_execution_date = execution_date - timedelta(hours = interval_hours)
    topic_partitions = [TopicPartition(topic_name, num_partition)]

    start_offsets = consumer.offsets_for_times({
        tp: int(prev_execution_date.timestamp() * 1000) for tp in topic_partitions
    })
    end_offsets = consumer.offsets_for_times({
        tp: int(execution_date.timestamp() * 1000) for tp in topic_partitions
    })
    
    return {"start_offset": start_offsets[topic_partitions[0]].offset, 
            "end_offset": end_offsets[topic_partitions[0]].offset}

def kafka_offset_search(**kwargs):
    ti = kwargs['ti']  # TaskInstance 객체
    execution_date = kwargs.get('execution_date', datetime.now())
    topic_name = kwargs['topic_name']
    num_partitions = kwargs['num_partitions']
    gcs_name = kwargs['gcs_name']
    kafka_bootstrap_server_list_file_name = kwargs['kafka_bootstrap_server_list_file_name']

    kafka_bootstrap_servers = download_blob(gcs_name, kafka_bootstrap_server_list_file_name)
    kafka_bootstrap_servers_str = ",".join(kafka_bootstrap_servers)
    offsets = get_kafka_offset(kafka_bootstrap_servers_str, topic_name, num_partitions, execution_date)

    ti.xcom_push(key='kafka_start_offsets', value=offsets['start_offset'])
    ti.xcom_push(key='kafka_end_offsets', value=offsets['end_offset'])
    print(offsets)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dags_spark_submit_bash_operator_batch_kafka_to_gcs_upbit_ob_tr',
    default_args=default_args,
    description='Submit PySpark Job to Dataproc',
    schedule_interval="*/5 * * * *",
    catchup=False,
)

search_upbit_orderbook_offset_task = PythonOperator(
    task_id='search_kafka_upbit_orderbook_offset',
    provide_context=True,
    python_callable=kafka_offset_search,
    op_kwargs={'topic_name': 'upbit_orderbook',
               'num_partitions': 0,
               'gcs_name': 'crypto-market-data-gcs',
               'kafka_bootstrap_server_list_file_name': 'kafka_broker_ips.txt'},
    dag=dag,
)

upbit_orderbook_spark_submit_command = """
gcloud dataproc jobs submit pyspark \
    gs://crypto-market-data-gcs/kafka_to_gcs_by_spark_batch.py \
    --cluster=spark-airflow \
    --region=asia-northeast3 \
    --properties  spark.dynamicAllocation.enabled=true,spark.shuffle.service.enabled=true,spark.dynamicAllocation.initialExecutors=1,spark.dynamicAllocation.minExecutors=1,spark.dynamicAllocation.maxExecutors=3,spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
    -- \
    --execution-date "{{ ds }}" \
    --kafka-bootstrap-server-list-file-name 'kafka_broker_ips.txt' \
    --topic-name 'upbit_orderbook' \
    --num-partitions '0' \
    --gcs-name 'crypto-market-data-gcs' \
    --gcs-save-path 'upbit/orderbook' \
    --app-name 'upbit-orderbook-save-to-gcs' \
    --kafka-start-offset "{{ task_instance.xcom_pull(task_ids='search_kafka_upbit_orderbook_offset', key='kafka_start_offsets') }}" \
    --kafka-end-offset "{{ task_instance.xcom_pull(task_ids='search_kafka_upbit_orderbook_offset', key='kafka_end_offsets') }}"
    """
upbit_orderbook_submit_pyspark_job = BashOperator(
    task_id='upbit_orderbook_submit_pyspark_job',
    bash_command=upbit_orderbook_spark_submit_command,
    dag=dag,
)


search_upbit_trade_offset_task = PythonOperator(
    task_id='search_kafka_upbit_trade_offset',
    provide_context=True,
    python_callable=kafka_offset_search,
    op_kwargs={'topic_name': 'upbit_trade',
               'num_partitions': 0,
               'gcs_name': 'crypto-market-data-gcs',
               'kafka_bootstrap_server_list_file_name': 'kafka_broker_ips.txt'},
    dag=dag,
)

upbit_trade_spark_submit_command = """
gcloud dataproc jobs submit pyspark \
    gs://crypto-market-data-gcs/kafka_to_gcs_by_spark_batch.py \
    --cluster=spark-airflow \
    --region=asia-northeast3 \
    --properties spark.dynamicAllocation.enabled=true,spark.shuffle.service.enabled=true,spark.dynamicAllocation.initialExecutors=1,spark.dynamicAllocation.minExecutors=1,spark.dynamicAllocation.maxExecutors=3,spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
    -- \
    --execution-date "{{ ds }}" \
    --kafka-bootstrap-server-list-file-name 'kafka_broker_ips.txt' \
    --topic-name 'upbit_trade' \
    --num-partitions '0' \
    --gcs-name 'crypto-market-data-gcs' \
    --gcs-save-path 'upbit/trade' \
    --app-name 'upbit-trade-save-to-gcs' \
    --kafka-start-offset "{{ task_instance.xcom_pull(task_ids='search_kafka_upbit_trade_offset', key='kafka_start_offsets') }}" \
    --kafka-end-offset "{{ task_instance.xcom_pull(task_ids='search_kafka_upbit_trade_offset', key='kafka_end_offsets') }}"
    """
upbit_trade_submit_pyspark_job = BashOperator(
    task_id='upbit_trade_submit_pyspark_job',
    bash_command=upbit_trade_spark_submit_command,
    dag=dag,
)

search_upbit_orderbook_offset_task >> upbit_orderbook_submit_pyspark_job >> search_upbit_trade_offset_task >> upbit_trade_submit_pyspark_job

# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# import pendulum

# default_args = {
#     'start_date': pendulum.datetime(2024, 3, 21),
#     'depends_on_past': False,
# }

# dag = DAG(
#     "dags_spark_submit_bash_operator_batch_kafka_to_gcs",
#     default_args=default_args,
#     schedule_interval="*/5 * * * *",
#     catchup=False
# )

# spark_submit_command = """
# spark-submit \
#     --master yarn \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
#     /home/bestech49/airflow-docker/plugins/spark_funcs/kafka_to_gcs_by_spark_batch.py \
#     --execution-date "{{ ds }}" \
#     --kafka-bootstrap-server-list-path '/home/bestech49/kafka_broker_ips.txt' \
#     --topic-name 'upbit_orderbook' \
#     --num_partitions '0' \
#     --schema-path '/home/bestech49/airflow-docker/plugins/files/schemas/upbit_orderbook.json' \
#     --gcs-name 'crypto-market-data-gcs' \
#     --gcs-save-path 'upbit/orderbook' \
#     --app-name 'upbit-orderbook-save-to-gcs'
# """
# print(spark_submit_command)
# spark_job = BashOperator(
#     task_id="spark_batch_operator_for_kafka_to_gcs",
#     bash_command=spark_submit_command,
#     dag=dag,
# )
