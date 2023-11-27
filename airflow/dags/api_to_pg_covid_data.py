from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from modules.fetch_api import FetchApi
from modules.transform import Transform

from datetime import datetime
import logging
import json

# -------------------- Set variable --------------------
config = Variable.get('api_to_pg_covid_data_config')
config = json.loads(config)

mysql_hook     = MySqlHook(mysql_conn_id=config['mysql_connection'])
pg_hook        = PostgresHook(postgres_conn_id=config['pg_connection'])
engine_mysql   = mysql_hook.get_sqlalchemy_engine()
engine_postgre = pg_hook.get_sqlalchemy_engine()

# -------------------- Callable of get data from api --------------------
def fun_get_data_from_api():
    fetch = FetchApi(config['api_url'])
    df    = fetch.get_data()
    df.to_sql(con=engine_mysql, name='covid_data',if_exists='replace', index=False)
    logging.info('Success to store API data to MySQL')
    
# -------------------- Callable of get data from api --------------------
def fun_generate_dim():
    transformer = Transform(engine_mysql, engine_postgre)
    transformer.create_dim_table('province')
    transformer.create_dim_table('district')
    transformer.create_dim_table('case')
    logging.info('Success to store API data to MySQL')

# -------------------- Callable of insert district daily --------------------
def fun_insert_district_daily():
    transformer = Transform(engine_mysql, engine_postgre)
    transformer.insert_daily('district')

# -------------------- Callable of insert province daily --------------------
def fun_insert_province_daily():
    transformer = Transform(engine_mysql, engine_postgre)
    transformer.insert_daily('province')

# -------------------- Initiate dag --------------------
with DAG(
    dag_id='api_to_pg_covid_data',
    start_date=datetime(2023, 10, 1),
    schedule_interval='0 23 * * *',          # Run Daily at 23:00  
    catchup=False,                           # True: Run from start_date, False: Run from the last date 
    max_active_runs=1
) as dag:

    # Operator Start
    op_start = EmptyOperator(
        task_id='start'
    )

    # Operator Check File
    op_get_data_from_api = PythonOperator(
        task_id='get_data_from_api',
        python_callable=fun_get_data_from_api
    )

    op_generate_dim = PythonOperator(
        task_id='generate_dim',
        python_callable=fun_generate_dim
    )

    op_insert_district_daily = PythonOperator(
        task_id='insert_district_daily',
        python_callable=fun_insert_district_daily
    )

    op_insert_province_daily = PythonOperator(
        task_id='insert_province_daily',
        python_callable=fun_insert_province_daily
    )

    # Operator End
    op_end = EmptyOperator(
        task_id='end'
    )

    op_start >> op_get_data_from_api >> op_generate_dim >> [op_insert_district_daily, op_insert_province_daily]>> op_end