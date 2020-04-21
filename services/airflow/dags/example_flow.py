import json
import requests
from airflow import DAG
from datetime import datetime, timedelta
from clickhouse_driver import Client
from airflow.operators.python_operator import PythonOperator
from data_models import database, Events_buf, Events
from multiprocessing import Pool
from math import ceil


# client = Client(host='127.0.0.1')
# client = Client('127.0.0.1')
# client = Client('http://localhost:9000')
client = Client('localhost')
reg_format_date = datetime.now().strftime("_%Y-%m-%d_%I-%M-%S")


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


# d_date = datetime.now()


def download_file(url):

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(f'datafile{reg_format_date}.json', 'wb') as jsonfile:
            for chunk in response.iter_content(chunk_size=1024 * 36):
                if chunk:
                    jsonfile.write(chunk)
                    jsonfile.flush()
    return jsonfile


def get_raw_data():
# def get_raw_data(file):
    # with open(file, 'r') as readfile:
    # with open('datafile-2020-04-21_12-37-28.json', 'r') as readfile:
    with open('datafile_2020-04-21_12-37-28.json', 'r') as readfile:
        data = [Events_buf(**json.loads(i.strip())) for i in readfile.readlines()]
        # database.insert(data)
    return data


def parting(list):
    part_len = ceil(len(list)/5)
    return [list[part_len*k:part_len*(k+1)] for k in range(5)]


def insert(data):
    database.insert(data)


def insert_rawdata():
    url = 'https://downloader.disk.yandex.ru/disk/d2c3c369a04f3368d117c3d06b5b0e319a3cb027c887ecc66319b72716cf59b9/5e9e4ca4/ktmvbmxs_zpIM4Z6o0NyqHpJ8l3qQgq2pkNZUcbRIjddCjG3yG407g4SRHbNQM1W-WWU1lmblgkjaxr4QBqtBw%3D%3D?uid=0&filename=event-data.json&disposition=attachment&hash=g2bRMWmdwB4RfjYLiYcxGBQMAZimnMYxlFQT0nH7zc6EIDSE7Z7xfNZRY23w%2BG/Cq/J6bpmRyOJonT3VoXnDag%3D%3D&limit=0&content_type=text%2Fplain&owner_uid=98963981&fsize=242618312&hid=2e6b145256a04c498b93fc9ede8f2597&media_type=text&tknv=v2'
    # data = parting(get_raw_data(download_file(url)))
    data = parting(get_raw_data())
    with Pool(5) as pool:
        pool.map(insert, data)

def transfer_data():
    client.execute('''
    INSERT into eventdata.events select 
    toDateTime(ts/1000)
    , userId
    , sessionId 
    , page 
    , auth 
    , method
    , status 
    , level
    , itemInSession 
    , userAgent 
    , location 
    , lastName 
    , firstName 
    , registration 
    , gender 
    , artist 
    , song
    , length
    from eventdata.events_buf
    ''')
    client.execute('truncate table eventdata.events_buf')


insert_rawdata()
transfer_data()

# with dag()
with DAG("example_airflow", default_args=default_args, schedule_interval=timedelta(1)) as dag:
    load_data = PythonOperator(
        task_id='load_raw_data',
        provide_context=True,
        python_callable=insert_rawdata
    )

    collect_data = PythonOperator(
        task_id='transfer_data',
        provide_context=True,
        python_callable=transfer_data
    )

    load_data.set_downstream(collect_data)
