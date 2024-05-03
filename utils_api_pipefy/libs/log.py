import pathlib
import re
from logging import StreamHandler, basicConfig, FileHandler
from datetime import datetime
import os
import dotenv
from sys import stdout
from urllib3.connectionpool import log as urllib_log

dotenv.load_dotenv(dotenv_path=fr"{os.getcwd()}\.env")

formatter = '[%(levelname)s]: [%(filename)s line - %(lineno)d] | Date_Time: %(asctime)s | Function: [%(funcName)s] | Message: ➪ %(message)s'
stdout_handler = StreamHandler(stdout)


def loginit(name_file_log: str = "GPC", dev_env: str = "DEV", disable_log: str | bool = True):
    """
    Inicializa a configuração de log.

    Args:
        name_file_log (str, opcional): Nome base para o arquivo de log. Padrão "GPC".
        dev_env (str, opcional): Indicador de ambiente (DEV ou PROD). Padrão "DEV".
        disable_log (bool, opcional): Se desabilita o log. Padrão True.
    """
    basicConfig( level=10, format=formatter, handlers=[stdout_handler], encoding='utf-8')
    
    if (disable_log == True) or (disable_log == "True"):
        urllib_log.disabled = True
        return
    
    valid_variables = list(set([name_file_log, dev_env, disable_log]))
    
    if valid_variables:
        
        filename_regex = re.compile(r"^[a-zA-Z0-9 -]{1,255}$")
        
        filename = f"Logs - {datetime.now().strftime('%d-%m-%Y %H')}.log"
        if filename_regex.match(name_file_log):
            filename = f"{name_file_log} - {datetime.now().strftime('%d-%m-%Y %H')}.log"
            
        new_filename = f'[DEV] {filename}' if dev_env == 'DEV' else f'[PROD] {filename}'
        dirname = f"{os.getcwd()}/logs"
        os.makedirs(dirname, exist_ok=True)
        dirname = f"{dirname}/{datetime.now().strftime('%d-%m-%Y')}"
        os.makedirs(dirname, exist_ok=True)
        full_filename = pathlib.Path(dirname) / new_filename
        
        file_handler = FileHandler(filename=full_filename, encoding='utf-8')
        basicConfig(level=10, format=formatter, handlers=[file_handler, stdout_handler], encoding='utf-8')
