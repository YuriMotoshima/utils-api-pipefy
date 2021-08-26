import sys
import os
sys.path.insert(0,os.getcwd())

import logging
from os import getcwd
from datetime import datetime

class Logger():

    def __init__(self, filename : str):
        self.logger = logging.getLogger(__name__) #pega o nome da modulo atual
        #logger.setLevel(logging.INFO) #determina que somento os log de INFO serão registrados no arquivo log

        formatter = logging.Formatter('%(levelname)s: %(filename)s line - %(lineno)d Date_Time: %(asctime)s Function: %(funcName)s Message: \n ==> %(message)s \n')
        file_handler = logging.FileHandler(filename=fr"{getcwd()}\log\logs\{filename} - Log - {datetime.now().strftime('%d-%m-%Y')}", encoding='utf-8') #determina o caminho e arquivo de log
        file_handler.setFormatter(formatter) #determina a formatação que deve ter cada linha do arquivo log
        
        self.logger.addHandler(file_handler) #faz escrever no arquivo log os logger.debug
        self.logger.setLevel(logging.DEBUG) #define o level do log, para que possa ser escrito no log

    def info(self, message: str):
        print(message)
        self.logger.info(message)


# import os
# import sys
# import logging
# from datetime import datetime
# from selenium.webdriver.remote.remote_connection import LOGGER as selenium_log
# from urllib3.connectionpool import log as urllib_log


# def loginit(name_file_log : str, dev_env : str = None):

#     filename = f'{name_file_log} - {datetime.now().strftime("%d-%m-%Y")}.txt'
#     filename = f'[DEV] {filename}' if dev_env == 'DEV' else filename

#     dirname = os.path.join(os.path.dirname(__file__), 'logs')
#     os.makedirs(dirname, exist_ok=True)
#     full_filename = os.path.join(dirname, filename)

#     formatter = '%(levelname)s: %(filename)s line - %(lineno)d Date_Time: %(asctime)s Function: %(funcName)s Message: \n ==> %(message)s \n'

#     file_handler = logging.FileHandler(filename=full_filename)
#     stdout_handler = logging.StreamHandler(sys.stdout)
#     handlers = [file_handler, stdout_handler]

#     logging.basicConfig(level=logging.DEBUG,
#                         format=formatter, handlers=handlers)

#     # Remove selenium and urlib3 log
#     if dev_env == 'PROD':
#         selenium_log.disabled = True
#         urllib_log.disabled = True