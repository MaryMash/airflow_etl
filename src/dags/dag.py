import json
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

task_logger = logging.getLogger("airflow.task")
http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

POSTGRES_CONN_ID = 'postgresql_de'
NICKNAME = 'm_grigoryeva'
COHORT = '17'

headers = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
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

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError('Increment is empty. Most probably due to error in '
                         'API call.')

    ti.xcom_push(key='increment_id', value=increment_id)
    task_logger.info(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{increment_id}/{filename}'
    task_logger.info(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    task_logger.info(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    task_logger.info(response.content)

    df = pd.read_csv(local_filename, index_col=0)
    df = df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()

    with conn.cursor() as cur:
        # удаление данных за дату инкремента, для того чтобы не было дублей
        request = f"DELETE from staging.user_order_log WHERE date_time = '{date}';"
        cur.execute(request)
        task_logger.info(f'Deleted period: {date}')

        # загрузка новых данных
        insert_request = "insert into staging.user_order_log (uniq_id, date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount, status) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

business_dt = '{{ ds }}'

with DAG(
        'sales_mart',
        default_args=args,
        description='Provide default dag for sprint3',
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

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

    update_d_tables = list()
    for i in ['d_item', 'd_customer', 'd_city']:
        update_d_tables.append(PostgresOperator(
            task_id=f"update_{i}",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=f"sql/mart.{i}.sql"
            )
        )

    (
            generate_report
            >> get_report
            >> get_increment
            >> upload_user_order_inc
            >> update_d_tables
            >> update_f_sales
    )
