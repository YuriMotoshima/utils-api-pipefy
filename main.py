import json
import logging
import time

from utils_api_pipefy import Engine, exceptions
from os import environ

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
        
        list_card_id = [ i[0] for i in data ]
        for card_id in list_card_id:
            eng.move_card_to_phase(card_id=card_id, destination_phase_id="37644")
            
        print(f"\n\nTempo total: {time.time()-a}\n\n")
        print()
    except Exception as err:
        raise exceptions(err)
