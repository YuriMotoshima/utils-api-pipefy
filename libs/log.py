import os
import sys
import logging
from datetime import datetime
from urllib3.connectionpool import log as urllib_log


def loginit(name_file_log : str, dev_env : str = None):

    filename = f'{name_file_log} - {datetime.now().strftime("%d-%m-%Y %H")}.log'
    filename = f'[DEV] {filename}' if dev_env == 'DEV' else filename

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
    if dev_env == 'PROD':
        urllib_log.disabled = True