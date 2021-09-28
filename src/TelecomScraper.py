###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################
# Для работы с табличными данными
import pandas as pd

# Для работы с регулярными выражениями
import re

# Для работы с браузером
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException

# Для работы с датой-временем
import time
import datetime
import pytz

# Обработка HTML
from bs4 import BeautifulSoup

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
###############################################################################################################################################

class TelecomScraper:
    def __init__(
        self
        ,mts_login
        ,mts_password
        ,inetvl_login
        ,inetvl_password
        ,megafon_login
        ,megafon_password
        ,engine
        ,conn 
        ,host
    ):
            """
            Функция для инциализации объекта класса.
            Параметры:
                engine - стркоа подключения к dwh.
                conn - строка подключения к DWH.
                login - логин.
                mts_login - логин МТС.
                mts_password - пароль МТС.
                inetvl_login - логин АЛЬЯНС.
                inetvl_password - пароль АЛЬЯНС.
                megafon_login - МЕГАФОН логин.
                megafon_password - МЕГАФОН пароль.
                host - IP хоста.
            Выход: 
                нет.
            """
            self.engine = engine
            self.conn=conn
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--remote-debugging-port=9222')
            self.driver = webdriver.Chrome(options = chrome_options)
            self.driver.set_page_load_timeout(240)
            self.driver.set_window_size(1920, 1080)
            self.mts_login = mts_login
            self.mts_password = mts_password
            self.inetvl_login = inetvl_login
            self.inetvl_password = inetvl_password
            self.megafon_login = megafon_login
            self.megafon_password = megafon_password

    def get_balance(self, id_value):
        """
        Функция для получения баланса из dwh.
        Вход: 
            id_value(int) - id текущего баланса.
        Выход: 
            result(int) - баланс из dwh.
        """

        query = """
        select 
             balance
        from balance.balance
        where id = {id_value}
        """.format(id_value = id_value)

# Получаем данные из dwh
        balance_df = pd.read_sql(
            sql = query,
            con = self.engine
        )
        
# Если таблица не заполнена, то возвращаем 0 и записываем в таблицу
        if balance_df.shape[0] == 0: 
            balance_df = pd.DataFrame({'id' : [id_value], 'balance' : [0]})
            balance_df.to_sql(
                name = 'balance'
                ,con = self.engine
                ,schema = 'balance'
                ,if_exists = 'append'
                ,index = False
            )
            result = 0
        else: 
            result = balance_df.balance[0]        
        return result

    def write_balance(self, current_balance, id_value):
        """
        Функция для записи текущего баланса.
        Вход: 
            current_balance(float) - текущий баланс.
            id_value(int) - id текущего баланса.
        Выход: 
            нет.
        """
        
        date_load = datetime.datetime.now(pytz.timezone('Asia/Vladivostok')).strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
        update balance.balance 
        set balance = {current_balance}, date_load = '{date_load}'
        where
           id = {id_value}
        """
        conn = self.conn
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()

    def get_inetvl_balance(self, id_value):
        """
        Функция для получения баланса "Альянс Телеком".
        Вход: 
            id_value(int) - id текущего баланса.
        Выход: 
            нет.
        """
        url = 'https://stat.inetvl.ru/'   
# Получаем текущий баланс
        current_balance = self.get_balance(id_value = id_value)
        try:
            self.driver.get(url) 
            wait(self.driver, 240).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, '.button-enter')))
            self.driver.find_element(By.CSS_SELECTOR, 'input#login_input').send_keys(self.inetvl_login)
            self.driver.find_element(By.CSS_SELECTOR, 'input#pwd_input').send_keys(self.inetvl_password)
            self.driver.find_element(By.CSS_SELECTOR, '.button-enter').click()
            wait(self.driver, 240).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, '.bill_tbl')))
            page_source = self.driver.page_source
            bsObj = BeautifulSoup(self.driver.page_source, 'html5lib') 
            balance_raw = bsObj.find('table', { "class" : 'bill_tbl'}).contents[1].find('th').text
            balance_raw = re.sub('\n|\t|руб.', '', balance_raw).strip()            
        except (NoSuchElementException, TimeoutException):
            balance_raw = current_balance        
        if float(balance_raw) != current_balance:
            self.write_balance(current_balance = balance_raw, id_value = id_value)
    
    def get_mts_balance(self, id_value):
        """
        Функция для получения баланса МТС
        Вход: 
            id_value(int) - id текущего баланса.
        Выход: 
            нет.
        """
        url = 'https://login.mts.ru/amserver/UI/Login?service=lk&loginWithNum=9147206219&goto=http%3A%2F%2Flk.ssl.mts.ru%2F%3Ffrom%3Dlogin.mts.ru'
# Получаем текущий баланс
        current_balance = self.get_balance(id_value = id_value)
        try:
            self.driver.get(url)
            wait(self.driver, 240).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, '.btn_login')))
            self.driver.find_element(By.CSS_SELECTOR, '#phone').send_keys(self.mts_login)
            self.driver.find_element(By.CSS_SELECTOR, '#password').send_keys(self.mts_password)
            self.driver.find_element(By.CSS_SELECTOR, '.btn_login').click()
            wait(self.driver, 240).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, '.parental-statistics-balance__item_info')))
            self.driver.get('https://lk.mts.ru/api/accountInfo/mscpBalance')
            time.sleep(2)
            key = self.driver.page_source
            key = re.findall('>".+"<', key)[0]
            key = re.sub('>|<|"', '', key)
            self.driver.get('https://lk.mts.ru/api/longtask/check/{key}?for=api/accountInfo/mscpBalance'.format(key = key))
            time.sleep(2)
            data_raw = self.driver.page_source            
            balance_raw = re.findall(r':[-,−]?\d{1,8}.\d{1,8},|[-,−]?\d{1,8},', data_raw)
            if len(balance_raw) > 0:
                balance = re.sub(',|:', '', balance_raw[0])
            else:
                balance = current_balance
        except(TimeoutException, NoSuchElementException) as e:
            balance = current_balance        
        if round(float(balance), 2) != current_balance:
            self.write_balance(current_balance = round(float(balance), 2), id_value = id_value)
            
    def get_megafon_balance(self, id_value):
        """
        Функция для получения баланса Мегафона.
        Вход: 
            id_value(int) - id текущего баланса.
        Выход: 
            нет.
        """
        url = 'https://lk.megafon.ru/login/'
# Получаем текущий баланс
        current_balance = self.get_balance(id_value = id_value)
        self.driver.get(url)
        wait(self.driver, 240).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, '.gadget_login_box_button')))
        self.driver.find_element(By.CSS_SELECTOR, '.ui-inputPhone-imask').send_keys(self.megafon_login)
        self.driver.find_element(By.CSS_SELECTOR, '.ui-inputText-input').send_keys(self.megafon_password) 
        self.driver.find_element(By.CSS_SELECTOR, '.gadget_login_box_button').click()
# Если находим рекламу, то закрываемм ее
        if self.driver.find_elements(By.CSS_SELECTOR, '.ng_personal_popup_close'):
            self.driver.find_element(By.CSS_SELECTOR, '.ng_personal_popup_close').click()
        try:        
            wait(self.driver, 60).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.gadget_account_dropdown_balance')))
            self.driver.find_element(By.CSS_SELECTOR, '.gadget_account_dropdown_balance').click()
            page_source = self.driver.page_source
            bsObj = BeautifulSoup(page_source, 'html5lib')
            balance_raw = bsObj.findAll('div', { "class" : 'gadget_account_block'})[1].text
            balance = re.findall(r'[-,−]?\d{,8},\d{,2} |[-,−]?\d{,8} ', balance_raw)
            balance = [i for i in balance if len(i) > 1]
            balance = balance[0].strip().replace(',', '.').replace('−', '-')               
        except TimeoutException:
            balance = current_balance        
        if balance != '' and float(balance) != current_balance: 
            self.write_balance(current_balance = balance, id_value = id_value)
        
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
        
    def data(self): 
        """
        Получаем данные Телеком. 
        Вход: 
            нет.
        Выход: 
            нет.
        """
# Получаем баланс Альянс
        self.get_inetvl_balance(id_value = 3)
        print('Получаем баланс Альянс')
# Получаем баланс МТС
        self.get_mts_balance(id_value = 2)
        print('Получаем баланс МТС')
# Получаем баланс Мегафон
        self.get_megafon_balance(id_value = 4)
        print('Получаем баланс Мегафон')
# Закрываем браузер
        self.close_all()
        print('Закрываем браузер')