from os import getenv, getcwd
from dotenv import load_dotenv
import pandas as pd
from libs.utilits_pipe import Pipe
from libs.logger import logger
from libs.engine import Engine

if __name__ == "__main__":
    
    load_dotenv(dotenv_path=fr"{getcwd()}\.env")

    TOKEN = getenv('TOKEN')
    HOST = getenv('HOST')
    PIPE = getenv('PIPE')
    NONPHASES = getenv('NONPHASES')


    eng = Engine(token=TOKEN, host=HOST, pipe=PIPE, nonphases=NONPHASES, logger=logger)
    
    df = pd.DataFrame(data=eng.run_all_data_phases(), columns=eng.columns)
    
    
    print("")
    