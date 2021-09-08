import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
from urllib3.connectionpool import log as urllib_log
# from selenium.webdriver.remote.remote_connection import LOGGER as selenium_log

load_dotenv(dotenv_path=fr"{os.getcwd()}\.env")
class log:
    def __init__(self, name_file_log : str = None, dev_env : str = None) -> None:
        self.name_file_log = os.getenv("LOGNAME") if name_file_log == None else name_file_log
        self.dev_env = os.getenv("LOGENV") if dev_env == None else dev_env
            
            
    def loginit(self):

        filename = f'{self.name_file_log} - {datetime.now().strftime("%d-%m-%Y %H")}.log'
        filename = f'[DEV] {filename}' if self.dev_env == 'DEV' else filename

        dirname = f'{os.getcwd()}\\logs\\{datetime.now().strftime("%d-%m-%Y")}'
        os.makedirs(dirname, exist_ok=True)
        full_filename = os.path.join(dirname, filename)

        formatter = '[%(levelname)s]: [%(filename)s line - %(lineno)d] | Date_Time: %(asctime)s | Function: [%(funcName)s] | Message: \n ==> %(message)s'

        file_handler = logging.FileHandler(filename=full_filename)
        stdout_handler = logging.StreamHandler(sys.stdout)
        handlers = [file_handler, stdout_handler]

        logging.basicConfig(level=logging.DEBUG,
                            format=formatter, handlers=handlers, )

        # Remove selenium and urlib3 log
        if self.dev_env == 'PROD':
            urllib_log.disabled = True