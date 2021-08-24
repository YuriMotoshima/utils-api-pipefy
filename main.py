from os import getenv, getcwd
from dotenv import load_dotenv
import pandas as pd
from libs.logger import logger
from libs.engine import Engine

from libs.parse_duplicate import DuplicatePipe

if __name__ == "__main__":

    load_dotenv(dotenv_path=fr"{getcwd()}\.env")

    TOKEN = getenv('TOKEN')
    HOST = getenv('HOST')
    PIPE = getenv('PIPE')
    NONPHASES = getenv('NONPHASES')

    eng = Engine(token=TOKEN, host=HOST, pipe=PIPE,
                 nonphases=NONPHASES, logger=logger)

    a = [{
        'card_id': '12132131', 'fields': {'sla': 'Card duplicado', 'dados_ok': 'Não', 'prazo_anatel': 'Card Improcedente'}
    }, {'card_id': '2312321342', 'fields': {'titulo': 'Card duplicado', 'etiqueta': 'Não', 'fe': 'Card Improcedente'}}]

    b = eng.run_update_fields_cards(data=a, automatic_editable="True")
    df = pd.DataFrame(data=eng.run_all_data_phases(), columns=eng.columns)

    print("")
