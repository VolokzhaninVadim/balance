###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Обработка HTML
from bs4 import BeautifulSoup

# Для работы с табличными данными
import pandas as pd

# Для работы с регулярными выражениями
import re

# Для работы с массивами и вычислениями
import numpy as np

# Для работы с браузером
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException

# Для работы с датой-временем
import datetime
import time
import pytz

# Для работы с операционной сисемой 
import os

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################

class AlfaScraper(): 
    def __init__(
        self,
        alfa_login
        ,alfa_password
        ,engine
        ,conn
        ,host
        ,user_data_dir = '/selenium'
        ,download_dir = '/share'
        ,websites = {
            'google' : 'https://messages.google.com/web/conversations/4'
            ,'alfa' : 'https://click.alfabank.ru/'
        }
    ):
        """
        Обработка данных сайта альфа-банка. 
        Вход: 
            alfa_login - логин Alfa.
            alfa_password - пароль Alfa.
            engine - строка подключения к DWH.
            conn - строка подключения к DWH.
            host - IP хоста.
            user_data_dir - директория сохранения настроек chrome.
            download_dir - директория сохранения выписок. 
            websites - словарь сайтов. 
        """
        self.alfa_login = alfa_login
        self.alfa_password = alfa_password
        self.engine = engine
        self.conn = conn
        self.user_data_dir = user_data_dir
        self.websites = websites
        self.download_dir = download_dir
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--remote-debugging-port=9222')
        chrome_options.add_argument(f'user-data-dir={self.user_data_dir}')
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory":  f'{self.download_dir}'
            ,"download.prompt_for_download": False
            ,"download.directory_upgrade": True
            ,"safebrowsing.enabled": True
        })
        self.driver = webdriver.Chrome(options = chrome_options)
        self.driver.set_page_load_timeout(60)
        self.driver.set_window_size(1920, 1080)
        
    def is_smartphone_available(self): 
        """
        Получение доступности смартфона. 
        Вход: 
            нет. 
        Выход: 
            result(bool) - признак наличия смартфона. 
        """
        comm = 'ping -c 5 192.168.0.169'
        response = os.popen(comm)
        data = response.readlines()
# Нам нужна вторая строка с результатом
        counter = 0
        for line in data:
            if counter > 0: 
                if 'ttl' in line:
                    result = True
                    break                        
                else: 
                    result = False
                    break
            counter += 1
        return result

    def close_all(self):
        """
        Закрытие драйвера Chrome.
        Вход: 
             нет.
        Выход: 
            нет. 
        """
        for window_handle in self.driver.window_handles:
            self.driver.switch_to.window(window_handle)
            self.driver.close()
        self.driver.quit()       

    def is_google_logged_in(self):   
        """
        Проверка авторизации.
        Вход: 
            driver(selenium.webdriver) - selenium.webdriver.
        Выход: 
            (bool) - признак прохождения авторизации.
        """
        self.driver.get(self.websites['google'])
        time.sleep(10)
        if 'Alfa Bank' in str(self.driver.page_source):
            self.check_use_pc()
            return True
        else:
            return False

    def check_use_pc(self): 
        """
        Проверяем необходимость подтверждения использования на ПК. 
        Вход: 
            нет.
        Выход: 
            нет.
        """    
        try: 
            self.driver.find_element_by_css_selector('button.action-button:nth-child(2)').click() 
        except NoSuchElementException as E: 
            pass
        
    def alfa_sms(self): 
        """
        Получаем последнюю смс от Alfa bank. 
        Вход: 
            нет. 
        Выход: 
            (int) - последняя смс от Alfa. 
        """
# Получаем последние смс Alfa
        if self.is_google_logged_in(): 
            bsObj = BeautifulSoup(self.driver.page_source, 'html5lib') 
            if bsObj.find_all('div', {'class' : 'bottom-anchored-scroll-pad'}): 
                sms_text = bsObj.find_all('div', {'class' : 'bottom-anchored-scroll-pad'})[0].text
                sms_list = re.findall('Пароль для входа - \d{1,10}', sms_text)
                if sms_list: 
                    sms_list = [int(re.sub('Пароль для входа - ', '', i)) for i in sms_list]
                    return sms_list[len(sms_list) - 1]
        else: 
            return None
    
    def html_to_accounts(self, bsObj): 
        """
        Получение из html значений. 
        Вход: 
            bsObj(BeautifulSoup) - html.
        Выход: 
            result(list) - список значений счетов.
        """
        if bsObj.find_all('div', {'class' : 'xwa'}): 
            text = bsObj.find_all('div', {'class' : 'xwa'})[0].text
            sallary = re.findall('1491.+?р', text)
            sallary = float(re.sub('1491|\s|р', '', sallary[0]))
            piggy_bank = re.findall('Копилка.+?р', text)
            piggy_bank = float(re.sub('Копилка|\s|р', '', piggy_bank[0]))
            slavery = re.findall('Ипотека.+?р', text)
            slavery = float(re.sub('Ипотека|\s|р', '', slavery[0]))
            result = [sallary, piggy_bank,slavery]
        else: 
            result = None
        return result
    
    def accounts(self):    
        """
        Получение данных с сайта "Альфа-банк". 
        Вход: 
            current_year(int) - текущий год. 
        Выход: 
            (list) - список остатков по счетам.
        """
        
# Заходим на страницу Alfa
        self.driver.execute_script(f"window.open('{self.websites['alfa']}');")
        self.driver.switch_to.window(self.driver.window_handles[1])
        self.driver.set_window_size(1920, 1080)
        time.sleep(10)
# Если нам не нужно логинится, то сразу выполняем сбор данных
        try: 
# Посылаем логин и пароль Alfa
            #self.driver.find_element_by_css_selector('#login-input').send_keys(f"{self.alfa_login}")
            self.driver.find_element_by_css_selector('.input__addons').click()
            time.sleep(10)
        
# Отсылаем пароль
            self.driver.find_element_by_css_selector('#password-input').send_keys(f"{self.alfa_password}")
            self.driver.find_element_by_css_selector('.input__addons').click()
            time.sleep(10)

# Переключаемся на первую вкладку с смс
            self.driver.switch_to.window(self.driver.window_handles[0])
# Получаем смс
            sms = self.alfa_sms()
            if sms: 
# Переключаемся на первую вкладку с Alafa и отсылаем пароль
                self.driver.switch_to.window(self.driver.window_handles[1])
                self.driver.find_element_by_css_selector('.input__control').send_keys(f"{sms}")
                time.sleep(10)

# Получаем данные по счетам
            bsObj = BeautifulSoup(self.driver.page_source, 'html5lib') 
            result = self.html_to_accounts(bsObj)
        except NoSuchElementException as e: 
            bsObj = BeautifulSoup(self.driver.page_source, 'html5lib') 
            result = self.html_to_accounts(bsObj)
        return result
    
    def get_balance(self):
        """
        Функция для получения баланса из dwh.
        Вход: 
            нет.
        Выход: 
            баланс из dwh.
        """

        query = """
        select 
                 id
                 ,balance
        from 
                balance.balance
        where 
                id in (5, 6, 7)
        order by
                id
        """

# Получаем данные из dwh
        balance_df = pd.read_sql(
            sql = query,
            con = self.engine
        )
        return balance_df
        
    def write_balance(self):
        """
        Запись балансов.
        Вход: 
            нет. 
        Выход: 
            нет.
        """

# Получаем текущий баланс
        current_balance_df = self.get_balance()

# Получаем баланс Alfa
        accounts_values = self.accounts()
        if accounts_values:
            balance_df = pd.DataFrame(data = accounts_values, columns = ['balance'])
            balance_df['id'] = [5, 6, 7]

# Ищем строки, которые не совпадают
            result_balance_df = current_balance_df.merge(right = balance_df, on = ['id'])
            result_balance_df['check'] = result_balance_df['balance_x'] == result_balance_df['balance_y']
            result_balance_df = result_balance_df[result_balance_df['check'] == False][['id', 'balance_y']].rename(columns = {'balance_y' : 'balance'})
            date_load = datetime.datetime.now(pytz.timezone('Asia/Vladivostok')).strftime("%Y-%m-%d %H:%M:%S")
            
# Записываем баланс, если есть строки для записи 
            if  result_balance_df.shape[0] > 0:
                for index, rows in result_balance_df.iterrows(): 
                    id_value = rows['id']
                    current_balance = rows['balance']
                    query = f"""
                    update balance.balance 
                    set balance = {current_balance}, date_load = '{date_load}'
                    where
                       id = {id_value}
                    """
                    with self.conn.cursor() as cursor:
                        cursor.execute(query)
                        self.conn.commit()

    def account(self): 
        """
        Получение данных из DWH. 
        Вход: 
            нет.
        Выход: 
            (DataFrame) - таблица с данными аккаунта. 
        """
        query = """
        select 
                account."date" 
                ,account."type"
                ,account.category 
                ,account.sum 
                ,account.currency
                ,account.account
                ,account.description
                ,account.comment
                ,account.date_load            
        from 
                balance.alfa_bank as account
        """
        return pd.read_sql(query, self.engine)
    
    def del_csv(self, current_year = datetime.datetime.now().year): 
        """
        Удаление csv.
        Вход: 
            csv_list(list) - список csv.
        Выход: 
            нет.
        """
# Получаем скаченные файлы
        list_files = os.listdir(self.download_dir)
        csv_list = list(filter(lambda x: re.findall('Budget_.+csv', x), list_files))
        for file in csv_list:
            os.remove(self.download_dir + '/' + file)

    def write_stayment(self, current_year = datetime.datetime.now().year): 
        """
        Получение выписки за год. 
        Вход: 
            нет. 
        Выход: 
            нет. 
        """

# Получаем данные из DWH 
        dwh_df = self.account()
        dwh_df['check'] = 'check'
# Получаем выписку        
        self.driver.get(f'https://click.alfabank.ru/pfm/hapi/export?dateFrom={current_year-1}-12-31T14:00:00.000Z&dateTo={current_year}-12-31T13:59:59.999Z')
        time.sleep(10)
        
# Получаем скаченные файлы
        list_files = os.listdir(self.download_dir)
        csv_list = list(filter(lambda x: re.findall('Budget_.+csv', x), list_files))

# Получаем выписки 
        account_statement_df = pd.DataFrame()
# Если файлы существуют, то обрабатываем и записываем их 
        if csv_list: 
            for file in csv_list: 
                curent_df = pd.read_csv(
                    filepath_or_buffer = self.download_dir + '/' + file
                    ,sep = ';'
                    ,dtype = {'Комментарий': 'object'}
                    ,error_bad_lines = False
                    ,engine="python"
                )
                account_statement_df = pd.concat([curent_df, account_statement_df])
                account_statement_df.columns = ['date', 'type', 'category', 'sum', 'currency', 'account', 'description', 'comment']
                account_statement_df['date'] = pd.to_datetime(account_statement_df['date'])
        
# Удаление csv
                self.del_csv()
        
# Ищем новые строки
                result_df = account_statement_df.merge(right = dwh_df, how = 'left', on = ['date', 'type', 'category', 'sum', 'currency', 'account', 'description'])
                result_df = result_df[result_df['check'].isna()]
                result_df['date_load'] = datetime.datetime.now(pytz.timezone('Asia/Vladivostok')).strftime("%Y-%m-%d %H:%M:%S")
                result_df.drop(['comment_y', 'check'], axis=1, inplace=True)
                result_df.rename(columns = {'comment_x' : 'comment'}, inplace = True)

# Записываем данные 
                if result_df.shape[0] > 0: 
                    result_df.to_sql(
                        name = 'alfa_bank'
                        ,schema = 'balance'
                        ,con = self.engine
                        ,if_exists = 'append'
                        ,index = False
                    )
            
    def data(self): 
        """
        Получаем данные с Альфа банка. 
        Вход: 
            нет.
        Выход: 
            нет.
        """
        if self.is_smartphone_available(): 

# Получаем и записываем баланс
            print('Получаем и записываем баланс, получаем файл выписки')
            self.write_balance()

# Получаем и записываем выписку
            print('Получаем и записываем выписку')
            self.write_stayment()

# Закрываем браузер
            print('Закрываем браузер')
            self.close_all()
        