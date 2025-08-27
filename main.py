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
        
        # data=eng.run_all_data_phases()
        
        card = eng.card(id=7641824)
        
        new_values = [
            { "fieldId": "texto_longo_vazio", "value": "texto_longo_vazio_cuan" },
            { "fieldId": "n_guia_no_prestador", "value": "11892332" }
            ]
        
        t = eng.update_fields_pipe(card_id=25030802, fields=new_values)
            
        print(f"\n\nTempo total: {time.time()-a}\n\n")
        print()
    except Exception as err:
        raise exceptions(err)
