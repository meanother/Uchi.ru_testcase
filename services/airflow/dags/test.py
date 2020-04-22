# import json
# import requests
from airflow import DAG
from datetime import datetime, timedelta
# from clickhouse_driver import Client
from airflow.operators.python_operator import PythonOperator
# from data_models import database, Events_buf, Events
# from multiprocessing import Pool
# from math import ceil




default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
from time import sleep
def action1(**kwargs):
    sleep(4)
    print('1')


def action2(**kwargs):
    sleep(5)
    print('2')
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parent_dir = os.path.dirname(os.path.abspath(__file__))
print(BASE_DIR)


with DAG("test_dag",
         default_args=default_args,
         schedule_interval='* * * * *') as dag:


    load_data = PythonOperator(
        task_id='load_raw_data',
        provide_context=True,
        python_callable=action1
    )

    collect_data = PythonOperator(
        task_id='transfer_data',
        provide_context=True,
        python_callable=action2
    )

    # load_data.set_downstream(collect_data)
load_data >> collect_data
# import requests
# import json
# url = 'https://ru.stackoverflow.com/questions/811008/%D0%A0%D0%B0%D0%B7%D0%B4%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5-%D1%81%D0%BF%D0%B8%D1%81%D0%BA%D0%B0-%D0%BD%D0%B0-%D1%87%D0%B0%D1%81%D1%82%D0%B8'
#
# response = requests.get(url, stream=True)
# if response.status_code == 200:
#     with open('qweqwe.txt', 'wb') as jsonfile:
#         for chunk in response.iter_content(chunk_size=1024 * 36):
#             if chunk:
#                 jsonfile.write(chunk)
#                 jsonfile.flush()
#         print(dir(jsonfile))
#         print(jsonfile.name)
#         print(BASE_DIR + '/' + jsonfile.name)
#         print(os.path.join(BASE_DIR), jsonfile.name)
#         print('-----------')
#         print(parent_dir + '/' + jsonfile.name)
#         print(os.path.join(parent_dir), jsonfile.name)
#
#         print(os.path.dirname(os.path.abspath(__file__)))
#         print(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + jsonfile.name)
# #
# #
# from clickhouse_driver import Client
#
# client = Client('localhost')
# print(client.execute('truncate table eventdata.events_buf'))
# print(client.execute('truncate table eventdata.events'))
