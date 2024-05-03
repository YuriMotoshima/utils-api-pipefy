import json
import logging
import os
import time

from utils_api_pipefy import Engine, exceptions

if __name__ == "__main__":
    
    try:
        eng = Engine()
        
        # ALGUMAS DAS UTILIDADES DO ENGINE
        logging.info(eng.columns)
        print(json.dumps(eng.phases_id, ensure_ascii=False, indent=2))
        print(json.dumps(eng.fields, ensure_ascii=False, indent=2))
        print(json.dumps(eng.phases, ensure_ascii=False, indent=2))
                
        a = time.time()
        data=eng.run_all_data_phases()
        print(f"\n\nTempo total: {time.time()-a}\n\n")
        print()
    except Exception as err:
        raise exceptions(err)
