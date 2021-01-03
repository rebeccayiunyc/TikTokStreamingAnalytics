from airflow import DAG
import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'retries':0,
}

with DAG('tiktok_dag',
         default_args = default_args,
         schedule_interval = '@once',
    start_date = datetime.datetime(2021, 1, 3),
         catchup=False) as dag:

    #task 1
    opr_dailybatch = BashOperator(task_id = 'dailyanalytics',
                                    bash_command = 'python3 /Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/static_tiktok.py ')

    #task 2
    opr_historywcstats = BashOperator(task_id = 'historywordcountstats',
                                      bash_command = 'python3 /Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/summarystats.py ')

    opr_dailybatch >> opr_historywcstats