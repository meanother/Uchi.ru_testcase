import json
import logging
import os
from datetime import datetime, timedelta
from math import ceil
from multiprocessing import Pool

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver import Client
from data_models import database, Events_buf

reg_format_date = datetime.now().strftime("_%Y-%m-%d_%I-%M-%S")
seven_days_ago = datetime.combine(datetime.today() - timedelta(3), datetime.min.time())

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + os.path.sep

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime.utcnow(),
    "start_date": seven_days_ago,
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


# TODO: MAKE A TIMER
# TODO: MAKE A ANOTHER METHOD INSERT DATA
# TODO CHANGE DATETIME REGION


def download_file(url):
    response = requests.get(url, stream=True)
    logging.info('this is default base dir: ' + str(BASE_DIR))
    if response.status_code == 200:
        logging.info('starting to download json file from yandex disk')
        with open(BASE_DIR + f'datafile{reg_format_date}.json', 'wb') as jsonfile:
            # TODO FULL FILE NAME EARLIER THEN >> WITH AS BECAUSE FILE SAVE IN /m BUT RETURN FULL PATH
            for chunk in response.iter_content(chunk_size=1024 * 36 * 512):
                if chunk:
                    jsonfile.write(chunk)
                    jsonfile.flush()
                    logging.info(f'flush chunk with size: {str((1024 * 36 * 512) / 1024 / 1024)} mb!')
    else:
        logging.error('close connect by invalid response server: ' + str(response.status_code))
        return False
    # TODO add time for download
    logging.info('finish download json file')
    return jsonfile.name


def get_raw_data(file):
    logging.info('started prepare dataset from json file')
    with open(file, 'r') as readfile:
        logging.info('parse json file')
        data = [Events_buf(**json.loads(i.strip())) for i in readfile.readlines()]
        # database.insert(data)
    logging.info('dataset is ready to insert to buf table')
    return data


def parting(list):
    logging.info('cut the list rows')
    part_len = ceil(len(list) / 5)
    return [list[part_len * k:part_len * (k + 1)] for k in range(5)]


def insert(data):
    database.insert(data)


def insert_rawdata():
    logging.info('start inserting data')
    url = 'https://downloader.disk.yandex.ru/disk/81590a98ae2d229706b093ba8f6d39722a2b15db1fd2083ceffac1d79c755a39/5ea230a0/ktmvbmxs_zpIM4Z6o0NyqHpJ8l3qQgq2pkNZUcbRIjddCjG3yG407g4SRHbNQM1W-WWU1lmblgkjaxr4QBqtBw%3D%3D?uid=0&filename=event-data.json&disposition=attachment&hash=g2bRMWmdwB4RfjYLiYcxGBQMAZimnMYxlFQT0nH7zc6EIDSE7Z7xfNZRY23w%2BG/Cq/J6bpmRyOJonT3VoXnDag%3D%3D&limit=0&content_type=text%2Fplain&owner_uid=98963981&fsize=242618312&hid=2e6b145256a04c498b93fc9ede8f2597&media_type=text&tknv=v2'
    data = parting(get_raw_data(download_file(url)))
    with Pool(3) as pool:
        logging.warning('MAP WITH POOL AS 5 WORKERS')
        pool.map(insert, data)
    logging.info('finish insert data to BUF table')


def transfer_data():
    client = Client('clickhouse')
    logging.info('Start insert data from BUF table to MAIN table')
    with open(BASE_DIR + 'from_buf.sql', 'r') as file:
        insert = file.read()
        client.execute(insert)
    logging.info('finish insert data to main table')
    logging.warning('Truncate BUF table')
    client.execute('truncate table eventdata.events_buf')


with DAG("example_airflow", default_args=default_args, schedule_interval=None) as dag:
    load_data = PythonOperator(
        task_id='load_raw_data',
        provide_context=False,
        python_callable=insert_rawdata
    )

    collect_data = PythonOperator(
        task_id='transfer_data',
        provide_context=False,
        python_callable=transfer_data
    )

load_data.set_downstream(collect_data)
# load_data >> collect_data
