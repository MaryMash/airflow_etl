import json
import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

task_logger = logging.getLogger("airflow.task")
http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host
business_dt = '{{ ds }}'

POSTGRES_CONN_ID = 'postgresql_de'
NICKNAME = 'm_grigoryeva'
COHORT = '17'
DATA_PATH = str(os.getcwd()) + '/data'

headers = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

args = {
    "owner": "m_grigoryeva",
    'email': ['test@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


def generate_report(ti):
    task_logger.info('Making request generate_report')
    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    task_logger.info(f'Response is {response.content}')


def get_report(ti):
    task_logger.info('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}',
                                headers=headers)
        response.raise_for_status()
        task_logger.info(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError("Time limit for report creation exceded")

    ti.xcom_push(key='report_id', value=report_id)
    task_logger.info(f'Report_id={report_id}')


def get_increment(date, ti):
    task_logger.info('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&'
        f'date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    task_logger.info(f'Response is {response.content}')

    data = json.loads(response.content)['data']

    increment_id = data['increment_id']
    if not increment_id:
        raise ValueError('Increment is empty. Most probably due to error in '
                         'API call.')

    increments = data['s3_path']

    path_exist = os.path.exists(DATA_PATH)
    if not path_exist:
        os.makedirs(DATA_PATH)

    for file_name in increments:
        local_filename = date + '_' + file_name + '.csv'
        response = requests.get(increments[file_name])
        response.raise_for_status()
        open(f"{DATA_PATH}/{local_filename}", "wb").write(response.content)
        task_logger.info(f'File {local_filename} saved')


def upload_inc_data_to_staging(filename, date, pg_table,
                               pg_schema, date_column, ti):
    if filename == 'customer_research_inc.csv':
        df = pd.read_csv(f"{DATA_PATH}/{date}_{filename}")
    elif filename == 'price_log_inc.csv':
        df = pd.read_csv(f"{DATA_PATH}/{date}_{filename}",
                         names=['product_name', 'price'])
    else:
        df = pd.read_csv(f"{DATA_PATH}/{date}_{filename}", index_col=0)

    if 'uniq_id' in df.columns:
        df = df.drop_duplicates(subset=['uniq_id'])

    if filename == 'user_order_log_inc.csv' and 'status' not in df.columns:
        df['status'] = 'shipped'

    cols = ', '.join(list(df.columns))
    values = ', '.join(['%s'] * len(list(df.columns)))

    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()

    with conn.cursor() as cur:
        # удаление данных за дату инкремента, для того чтобы не было дублей
        if pg_table == 'price_log':
            request = (f"DELETE from {pg_schema}.{pg_table}")
        else:
            request = (f"DELETE from {pg_schema}.{pg_table} \
                       WHERE {date_column} = '{date}';")
        cur.execute(request)
        task_logger.info(f'Deleted period: {date}')

        # загрузка новых данных
        insert_request = f'''insert into {pg_schema}.{pg_table} ({cols})
                             values ({values})'''
        task_logger.info(f'Request: {insert_request}')
        i = 0
        step = 100
        while i <= df.shape[0]:
            task_logger.info('batch: ' + str(i))
            batch_df = df.loc[i:i + step]
            data_tuples = [tuple(x) for x in batch_df.to_numpy()]
            task_logger.info(insert_request, data_tuples)
            if data_tuples:
                cur.executemany(insert_request, data_tuples)
                conn.commit()
            i += step+1

    cur.close()
    conn.close()


with DAG(
        'sales_mart_updated',
        default_args=args,
        catchup=True,
        start_date=datetime.today() - timedelta(days=1),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    with TaskGroup(group_id='sensors') as gr1:
        fs_1 = FileSensor(
            task_id='waiting_for_customer_research_inc',
            fs_conn_id="fs_local",
            filepath=(DATA_PATH + '/' + business_dt +
                      '_customer_research_inc.csv'),
            poke_interval=5,
            timeout=60 * 30,
            mode='reschedule'
        )

        fs_2 = FileSensor(
            task_id='waiting_for_user_order_log_inc',
            fs_conn_id="fs_local",
            filepath=DATA_PATH + '/' + business_dt + '_user_order_log_inc.csv',
            poke_interval=5,
            timeout=60 * 30,
            mode='reschedule'
        )

        fs_3 = FileSensor(
            task_id='waiting_for_user_activity_log_inc',
            fs_conn_id="fs_local",
            filepath=(DATA_PATH + '/' + business_dt +
                      '_user_activity_log_inc.csv'),
            poke_interval=5,
            timeout=60 * 30,
            mode='reschedule'
        )

        fs_4 = FileSensor(
            task_id='waiting_for_price_log_inc',
            fs_conn_id="fs_local",
            filepath=DATA_PATH + '/' + business_dt + '_price_log_inc.csv',
            poke_interval=5,
            timeout=60 * 30,
            mode='reschedule'
        )

    load_increment_data = list()
    for i in ['user_order_log', 'customer_research',
              'user_activity_log', 'price_log']:
        load_increment_data.append(
            PythonOperator(
                task_id=f'upload_{i}_inc',
                python_callable=upload_inc_data_to_staging,
                op_kwargs={'date': business_dt,
                           'filename': f'{i}_inc.csv',
                           'pg_table': i,
                           'pg_schema': 'staging',
                           'date_column': 'date_id' if i == 'customer_research'
                                          else 'date_time'})
        )
    update_f_tables = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="/sql/mart/f_sales.sql",
        parameters={"date": {business_dt}}
    )

    update_d_tables = list()
    for i in ['d_item', 'd_customer', 'd_city']:
        update_d_tables.append(PostgresOperator(
            task_id=f"update_{i}",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=f"/sql/mart/{i}.sql"
            )
        )
    (
            generate_report
            >> get_report
            >> get_increment
            >> gr1
            >> load_increment_data
            >> update_f_tables
            >> update_d_tables

    )
