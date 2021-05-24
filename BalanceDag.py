###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator
import datetime

# Для получения балансов Телеком
from balance.src.TelecomScraper import TelecomScraper
# Для получения данных Альфа-банк
from balance.src.AlfaScraper import AlfaScraper

# Для работы с операционной сисемой 
import os

# Для работы с SQL|
from sqlalchemy import create_engine
# Для работы с Postgre
import psycopg2

# Получаем переменные окружения
PG_HOST = os.environ['PG_HOST']
PG_PASSWORD = os.environ['PG_PASSWORD']
LOGIN_NAME = os.environ['LOGIN_NAME']
ALFA_USER = os.environ['ALFA_USER'] 
ALFA_PASSWORD = os.environ['ALFA_PASSWORD'] 
MTS_LOGIN=os.environ['MTS_LOGIN']
MTS_PASSWORD=os.environ['MTS_PASSWORD']
INETVL_LOGIN=os.environ['INETVL_LOGIN']
INETVL_PASSWORD=os.environ['INETVL_PASSWORD']
MEGAFON_LOGIN=os.environ['MEGAFON_LOGIN']
MEGAFON_PASSWORD=os.environ['MEGAFON_PASSWORD']


# Создаем подключение к pg
engine = create_engine(f'postgres://{LOGIN_NAME}:{PG_PASSWORD}@{PG_HOST}:5432/{LOGIN_NAME}')
conn = psycopg2.connect(f"host='{PG_HOST}' dbname='{LOGIN_NAME}' user='{LOGIN_NAME}' password='{PG_PASSWORD}'")

# Создаем объект класса
def alfa_scraper():
    """
    Получаем и записываем баланс Альфа-Банк.
    Вход: 
        нет.
    Выход: 
        нет.
    """
    alfa_scraper = AlfaScraper( 
        alfa_login = ALFA_USER
        ,alfa_password = ALFA_PASSWORD
        ,engine = engine
        ,conn = conn
        ,host = PG_HOST
        )
    alfa_scraper.data()

def telecom_scraper(): 
    """
    Получаем и записываем баланс Телеком.
    Вход: 
        нет.
    Выход: 
        нет.
    """
    telecom_scraper = TelecomScraper(
        mts_login=MTS_LOGIN
        ,mts_password=MTS_PASSWORD
        ,inetvl_login=INETVL_LOGIN
        ,inetvl_password=INETVL_PASSWORD
        ,megafon_login=MEGAFON_LOGIN
        ,megafon_password=MEGAFON_PASSWORD
        ,engine=engine
        ,conn=conn
        ,host=PG_HOST
        )
    telecom_scraper.data()

# Вводим по умолчанию аргументы dag
default_args = {
    'owner': 'Volokzhanin Vadim',
    'start_date': datetime.datetime(2020, 9, 17),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes = 2)
}

##############################################################################################################################################
############################################### Создадим DAG и поток данных ##################################################################
############################################################################################################################################## 
with DAG(
    "balance_loader", 
    description = "Получение балансов"
    ,default_args = default_args
    ,catchup = False
    ,schedule_interval = "0 */5 * * *"
    ,tags=['balance_loader']) as dag:

# Получаем баланс Альфа-Банк
    balance_alfa = PythonOperator(
        task_id = "balance_alfa", 
        python_callable = alfa_scraper, 
        dag = dag
        ) 
# Получаем баланс Телеком
    balance_telecom = PythonOperator(
        task_id = "balance_telecom", 
        python_callable = telecom_scraper, 
        dag = dag
        ) 

# Порядок выполнения задач
    balance_alfa >> balance_telecom  