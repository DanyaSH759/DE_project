import os
import logging
import shutil
import pytz
import pandas as pd
import psycopg2

from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago


# логи
_LOG = logging.getLogger()
_LOG.addHandler(logging.StreamHandler())

# параметры дага
DEFAULT_ARGS = {
    "owner": "Shulyak_Danila",
    "retry": 3,
    "retry_delay": timedelta(minutes=1),
}


# Функция для выполнения SQL-запроса
def send_sql_query_to_db(query):
    # Подключение к базе данных PostgreSQL
    engine = create_engine("postgresql+psycopg2://hseguest:hsepassword@rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net:6432/db")

    # Проверка соединение
    with engine.connect() as conn:
        _LOG.info("Соединение c бд установлено")

    try:
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(query)
                trans.commit()
                _LOG.info("SQL-скрипт выполнен успешно.")
            except Exception as e:
                trans.rollback()
                _LOG.info(f"Ошибка при выполнении SQL-скрипта: {e}")
                raise
    except Exception as e:
        _LOG.info(f"Ошибка подключения или выполнения транзакции: {e}")

    engine.dispose()


# Чтение SQL-скрипта из файла и выполнение
def execute_sql_file(file_path):
    try:
        with open(file_path, 'r') as file:
            sql_script = file.read()
        query = text(sql_script)
        send_sql_query_to_db(query)
    except Exception as e:
        _LOG.info(f"Ошибка при чтении или выполнении SQL-скрипта: {e}")
        raise

# функция для загрузки данных из exсel и txt файлов которые мы получаем
def load_to_dwh(stg_table, dwh_table, primary_key):

    if stg_table in ['shds_stg_passport_blacklist', 'shds_stg_terminals']:
        # меняем флаки на false если появилась новая запись по старым данным 
        query = text(f"""
            UPDATE {dwh_table}
            SET is_active = False
            WHERE {primary_key} IN (SELECT {primary_key} FROM {stg_table})
            AND is_active = True;
        """)

        send_sql_query_to_db(query)

        query = text(f"""
            INSERT INTO {dwh_table}
            SELECT *, True AS is_active, NOW()::timestamp AS load_date
            FROM {stg_table}
        """)

        send_sql_query_to_db(query)

    else:
        # транзакции достаточно просто загрузить 
        query = text(f"""
            INSERT INTO {dwh_table}
            SELECT *, NOW()::timestamp AS load_date
            FROM {stg_table}
        """)
        send_sql_query_to_db(query)


# функция по переносу файлов из data в backup
def move_and_rename_to_backup(file_path, backup_folder):
    try:
        # Создаем папку, если она еще не существует
        os.makedirs(backup_folder, exist_ok=True)

        # Получаем имя файла
        filename = os.path.basename(file_path)

        # Новое имя файла в папке backup с расширением .backup
        new_file_path = os.path.join(backup_folder, f"{filename}.backup")

        # Перемещаем и переименовываем файл
        shutil.move(file_path, new_file_path)
        _LOG.info(f"Файл {file_path} успешно перемещен и переименован в {new_file_path}.")
    except Exception as e:
        _LOG.info(f"Ошибка при перемещении и переименовании файла {file_path}: {e}")


# функция с циклом по перебору файлов из папки data
def process_files(input_folder, backup_folder):

    # Получение всех файлов в папке
    files = [os.path.join(input_folder, f) 
         for f in os.listdir(input_folder) 
         if os.path.isfile(os.path.join(input_folder, f)) and not f.startswith('~$')]

    for file_path in files:
        # Определяем таблицу в зависимости от имени файла
        if 'passport_blacklist' in file_path:
            stg_table = 'shds_stg_passport_blacklist'
            dwh_table = 'shds_dwh_dim_passport_blacklist'
            primary_key = 'passport'

            df = pd.read_excel(file_path)

        elif 'terminals' in file_path:
            stg_table = 'shds_stg_terminals'
            dwh_table = 'shds_dwh_dim_terminals'
            primary_key = 'terminal_id'

            df = pd.read_excel(file_path)

        elif 'transactions' in file_path:
            stg_table = 'shds_stg_transactions'
            dwh_table = 'shds_dwh_dim_transactions'
            primary_key = 'transaction_id'

            df = pd.read_csv(file_path, sep = ';')
            df['transaction_id'] = df['transaction_id'].astype('object')
            df['amount'] = df['amount'].str.replace(',', '.').astype('float')
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%Y-%m-%d %H:%M:%S')

        else:
            print(f"Неизвестный формат файла: {file_path}")
            continue
        
        # Подключение к базе данных через psycopg2
        conn = psycopg2.connect(
            dbname="db",
            user="hseguest",
            password="hsepassword",
            host="rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
            port="6432"
        )

        #Очистка таблицы перед загурзкой данных
        q_t = text(f"""
                   TRUNCATE public.{stg_table}
                   ;""")
        send_sql_query_to_db(q_t)

        # Курсор для выполнения операций
        cur = conn.cursor()

        # Преобразование DataFrame в CSV строку
        df.to_csv('/tmp/temp_file.csv', index=False, header=False, sep=';')

        # Открытие CSV файла и копирование данных в таблицу
        with open('/tmp/temp_file.csv', 'r') as f:
            cur.copy_from(f, stg_table, sep=';')

        # Коммит транзакции и закрытие подключения
        conn.commit()
        cur.close()
        conn.close()

        # Шаг 2: Перенос данных из STG в DWH
        load_to_dwh(stg_table, dwh_table, primary_key)
        
        # Шаг 3: Перемещение и переименование файла в .backup
        move_and_rename_to_backup(file_path, backup_folder)



# запуск функция через даги
def init():
    # логирование
    start = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%A, %D, %H:%M")
    _LOG.info(f'Время запуска {start }')

def insert_dwh_in_db():
    # Выполнение SQL-скрипта из файла
    execute_sql_file('/opt/airflow/sql_skripts/insert_data_db_dwh.sql')

def insert_data_from_csv():
    input_folder = '/opt/airflow/data/'
    backup_folder = "/opt/airflow/backup/"
    process_files(input_folder, backup_folder)

def  insert_data_rep_fraud():
    # Выполнение SQL-скрипта из файла для заполнения таблицы shds_rep_fraud
    execute_sql_file('/opt/airflow/sql_skripts/insert_data_rep_fraud.sql')


dag = DAG(
    dag_id = 'insert_data_table',
    schedule_interval = "0 1 * * *",
    start_date = days_ago(2),
    catchup = False,
    tags = ["insert"],
    default_args = DEFAULT_ARGS
)

task_init = PythonOperator(task_id="init", python_callable=init, dag = dag,)
task_insert_data_1 = PythonOperator(task_id="insert_data_1", python_callable=insert_dwh_in_db, dag=dag)
task_insert_data_2 = PythonOperator(task_id="insert_data_2", python_callable=insert_data_from_csv, dag=dag)
task_insert_data_3 = PythonOperator(task_id="insert_data_3", python_callable=insert_data_rep_fraud, dag=dag)

task_init >> task_insert_data_1 >> task_insert_data_2 >> task_insert_data_3
