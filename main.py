from os import getenv, getcwd
from dotenv import load_dotenv
import pandas as pd
from libs.log import loginit
from libs.engine import Engine

if __name__ == "__main__":

    load_dotenv(dotenv_path=fr"{getcwd()}\.env")
    loginit(name_file_log="Constructor")
   
    eng = Engine()
    df = pd.DataFrame(data=eng.run_all_data_phases(), columns=eng.columns)

    print("")
