from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timedelta
import pytz
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator


# логи
_LOG = logging.getLogger()
_LOG.addHandler(logging.StreamHandler())

DEFAULT_ARGS = {
    "owner": "Shulyak_Danila",
    "retry": 3,
    "retry_delay": timedelta(minutes=1),
}

# Подключение к базе данных PostgreSQL
engine = create_engine("postgresql+psycopg2://hseguest:hsepassword@rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net:6432/db")

# Проверьте соединение
with engine.connect() as conn:
    _LOG.info("Соединение c бд установлено")


# Функция для выполнения SQL-запроса
def send_sql_query_to_db(query):
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

def chek_table_in_db():

    chek_list_table = ["shds_stg_accounts", "shds_stg_cards", "shds_stg_clients",
                   "shds_stg_passport_blacklist", "shds_stg_terminals", "shds_stg_transactions",
                     "shds_dwh_dim_accounts", "shds_dwh_dim_cards", "shds_dwh_dimclients",
                   "shds_dwh_dim_passport_blacklist", "shds_dwh_dim_terminals", "shds_dwh_dim_transactions"]

    for i in chek_list_table:
        # Проверяем наличие таблицы
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = '{i}';
                """))
                if result.fetchone():
                    _LOG.info(f"Таблица {i} существует в базе данных.")
                else:
                    _LOG.info(f"Таблица {i} не найдена.")
        except Exception as e:
            _LOG.info(f"Ошибка при проверке наличия таблицы: {e}")

def init():
    # логирование
    start = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%A, %D, %H:%M")
    _LOG.info(f'Время запуска {start }')

def create_truncate_table():
    # Выполнение SQL-скрипта из файла
    # execute_sql_file('/opt/airflow/dags/create_stage_table.sql')
    execute_sql_file('/opt/airflow/sql_skripts/create_stage_table.sql')

def chek_table():

    chek_table_in_db()


dag = DAG(
    dag_id = 'create_table',
    schedule_interval = "0 1 * * *",
    start_date = days_ago(2),
    catchup = False,
    tags = ["create"],
    default_args = DEFAULT_ARGS
)

task_init = PythonOperator(task_id="init", python_callable=init, dag = dag,)
task_get_data = PythonOperator(task_id="get_data", python_callable=create_truncate_table, dag=dag)
task_prepare_data = PythonOperator(task_id="prepare_data", python_callable=chek_table_in_db, dag=dag)

task_init >> task_get_data >> task_prepare_data