import os
import json
import logging
from dotenv import load_dotenv
from utils_api_pipefy.libs.engine import Engine
from utils_api_pipefy.libs.excepts import exceptions
from utils_api_pipefy.libs.log import log

load_dotenv(dotenv_path=fr"{os.getcwd()}\.env")
log().loginit()

if __name__ == "__main__":
    
    try:
        eng = Engine()
        
        # ALGUMAS DAS UTILIDADES DO ENGINE
        logging.info(eng.columns)
        print(json.dumps(eng.phases_id, ensure_ascii=False, indent=2))
        print(json.dumps(eng.fields, ensure_ascii=False, indent=2))
        print(json.dumps(eng.phases, ensure_ascii=False, indent=2))
        
        data=eng.run_all_data_phases()
    
    except Exception as err:
        raise exceptions(err)
