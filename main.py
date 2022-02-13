from os import environ
import pandas as pd
import logging
from utils_api_pipefy.libs.log import log
from utils_api_pipefy.libs.engine import Engine

if __name__ == "__main__":
    
    log().loginit()
    
    eng = Engine()
    # ALGUMAS DAS UTILIDADES DO ENGINE
    logging.info(eng.columns)
    print(eng.phase_id)
    # print(eng.fields)
    # print(eng.phases)
    
    # df = pd.DataFrame(data=eng.run_all_data_phases(), columns=eng.columns)
    
    print("")
