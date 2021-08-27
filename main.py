from os import getenv, getcwd
from dotenv import load_dotenv
import pandas as pd
from log.log import loginit
from libs.engine import Engine

from libs.parse_duplicate import DuplicatePipe

if __name__ == "__main__":

    load_dotenv(dotenv_path=fr"{getcwd()}\.env")
    loginit(name_file_log="Constructor")

   
    eng = Engine()
    df = pd.DataFrame(data=eng.run_all_data_phases(), columns=eng.columns)

    print("")
