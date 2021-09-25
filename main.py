from os import getenv, getcwd
from dotenv import load_dotenv
import pandas as pd
from libs.log import log
from libs.engine import Engine

if __name__ == "__main__":
    
    load_dotenv(dotenv_path=fr"{getcwd()}\.env")
    log().loginit()
   
    eng = Engine()
    # ALGUMAS DAS UTILIDADES DO ENGINE
    print(eng.columns)
    print(eng.phase_id)
    print(eng.fields)
    print(eng.phases)
    
    df = pd.DataFrame(data=eng.run_all_data_phases(), columns=eng.columns)
    
    print("")
