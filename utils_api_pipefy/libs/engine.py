from os import environ, getcwd
import logging
from typing import NoReturn
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from dotenv import load_dotenv

from utils_api_pipefy.libs.excepts import exceptions
from utils_api_pipefy.libs.utilits_pipe import Pipe

load_dotenv(dotenv_path=f"{getcwd()}/.env")
class Engine(Pipe):
    def __init__(self, TOKEN=None, HOST=None, PIPE=None, NONPHASES=None):
        self.TOKEN = TOKEN or environ.get('TOKEN')
        self.HOST = HOST or environ.get('HOST_PIPE')
        self.PIPE = PIPE or environ.get('PIPE')
        self.NONPHASES = NONPHASES or environ.get('NONPHASES')
        
        super().__init__(token=self.TOKEN, host=self.HOST, pipe=self.PIPE, nonphases=self.NONPHASES)


    def _run_enable_disable_fields(func):
        def parser_enable_disable(args, kwargs) -> tuple:
            data = list(set([x['fieldId'] for n in kwargs["data"] for x in n["fields"]]))
            list_enable = []
            list_disable = []

            for first_id in args[0].fields["fields"]:
                if first_id["id"] in data and first_id["editable"] == False:
                    list_enable.append('{id: "%s", label: "%s", editable: %s, uuid: "%s"}' % (first_id["id"], first_id["nameField"], "true", first_id["uuid"]) )
                    list_disable.append('{id: "%s", label: "%s", editable: %s, uuid: "%s"}' % (first_id["id"], first_id["nameField"], "false", first_id["uuid"]) )
            return list_enable, list_disable
        
        def run(*args, **kwargs):
            start = datetime.now()
            print(f"{func.__name__} iniciado às {start}.")
            
            lte, ltd = parser_enable_disable(args, kwargs) if kwargs["automatic_editable"] else (None,None)
            
            if lte:
                try:
                    args[0].change_properties_fields(data=lte)
                    func(args[0], kwargs["data"])
                except Exception as e:
                    raise exceptions(e)
                finally:
                    args[0].change_properties_fields(data=ltd)
            else:
                func(args[0], kwargs["data"])
            
            end = datetime.now() - start
            print(f"{func.__name__} finalizado às {datetime.now()}.\nTempo de execução (hh:mm:ss.ms) {end}")
        return run
    
    
    def run_all_data_phases(self) -> list:
        """
        Função "motor" de chamadas paralelizadas, feita para extratação de várias fases ao mesmo tempo, de acordo com os dados passados no arquivo .env.
        
        Criado uma Thread para cada nucleo de processador:
            - Cada thread é chamado a extração de uma fase.
            - Cada thread completada é verificado o E acrescentado em uma lista
            
        Retorna uma lista com contendo tuplas com os dados de cada card de cada fase consultada.
        
        `Exemplo:` [('523936', ' - TESTE - !aSA2@#sa...0239988322', 'Validação', None, '2021-08-09T16:07:40-03:00', 'Yuri Roberto Motoshima', [...], None, '2021-08-09T15:37:40-03:00', ...)]
        """
        try:
            dados = []
            
            with ProcessPoolExecutor() as exe:
                list_future = []
                
                for phase in self.phases_id:
                    worker = exe.submit(self.get_data_phase, phase)
                    list_future.append(worker)
                    logging.info(f"Phase: {phase}, Worker Running: {worker.running()}.")
                    
                for worker in as_completed(list_future):
                    if worker.result().get("Status"):
                        dados.extend(worker.result().get("Data"))
            
            return dados
        except Exception as e:
            logging.info(e)
            raise exceptions(e)


    @_run_enable_disable_fields
    def run_update_fields_cards(self, data : list, automatic_editable : bool = False) -> NoReturn:
        """
        Função "motor" de chamadas paralelizadas, feita para atualizar campos de vários cards ao mesmo tempo, de acordo com os dados passadas.
        
        Enviar em `data`:
        
            - card_id <== Número do card a ser atualizado
            - fields <== [{}] Lista com Dicionários informando o fieldId do field e value a ser preenchido.
            
        Exemplo `data`:
            [{
                "card_id":"12132131",
                "fields":[ 
                        { "fieldId": "ultimo_after_api", "value": f'{value_ultimo_after_api}' }, 
                        { "fieldId": "fases_restantes", "value": f'{value_fases_restantes}' }
                        ]
            }]
        
        """
        try:
            if data:
                with ProcessPoolExecutor() as exe:
                    for key in data:
                        card_id = key.get("card_id")
                        fields = key.get("fields")
                        exe.submit(self.update_fields_pipe, card_id, fields)
                        
        except Exception as e:
            logging.info(e)
            raise exceptions(e)
    
     
    def run_delete_all_cards(self) -> NoReturn:
        """
        Função "motor" de chamadas paralelizadas, feita para excluir cards em massa do Pipefy.
        """
        try:
            cards = self.run_all_data_phases()
            msg = int(input(f"Você está prestes a excluir TODOS os cards ({len(cards)}) do Pipefy:{self.PIPE}.\nDeseja continuar (0 - Não ou 1 - Sim )?")) 
            if msg == 1:
                with ProcessPoolExecutor() as exe:
                    list_future = []
                    for card in cards:
                        print(card[0])
                        worker = exe.submit(self.delete_cards, card[0])
                        list_future.append(worker)
                        logging.info(f"\nCard_ID: {card[0]}, Worker Running: {worker.running()}.")
                        
            elif msg == 0:
                logging.info(f"Operação cancelada !")
                
            else:
                logging.info(f"Opção incorreta, processo cancelado!")
                                  
        except Exception as e:
            logging.info(e)
            raise  exceptions(e)

   
    def run_all_cards_filtered(self, first_date : str , operator : str, conditional : str = None, sec_operator : str = None, second_date : str = None):
        """
        Função `motor` de chamada que pegar os cards de acordo com a data de atualiação.
        
        Variaveis:
            - `first_date` e `second_date` : str = "2021-08-26 10:00:00" --> "2021-08-26T10:00:00-03:00"
            - `operator` e `sec_operator` : str
                - `equal` = Equals to  
                - `gt` = Greater than 
                - `gte` = Greater than or equal to 
                - `lt` = Less than 
                - `lte` = Less than or equal to
            - `conditional` : str = AND / OR
            
        Exemplo do filtro que será criado e enviado na chamada: 'filter: {field: "updated_at", operator: gte, value: "2021-08-26T10:00:00-03:00", AND: [{field: "updated_at", operator: lt, value: "2021-08-26T11:00:00-03:00"}]}'
        
        Retorna uma lista com contendo tuplas com os dados de cada card de cada fase consultada.
        
        `Exemplo:` [('523936', ' - TESTE - !aSA2@#sa...0239988322', 'Validação', None, '2021-08-09T16:07:40-03:00', 'Yuri Roberto Motoshima', [...], None, '2021-08-09T15:37:40-03:00', ...)] 
        
        """
        try:
            date_format = lambda a : f'{a.split()[0]}T{a.split()[1]}-03:00'
            
            if conditional and sec_operator and second_date:
                conditional_filter = 'filter: {field: "updated_at", operator: %s, value: "%s", %s: [{field: "updated_at", operator: %s, value: "%s"}]}' % (operator, date_format(first_date), conditional, sec_operator, date_format(second_date))
            else:
                conditional_filter = 'filter: {field: "updated_at", operator: %s, value: "%s"}' % (operator, date_format(first_date))
                
            dados = []
            
            dados.extend(self.get_data_cards_filter(pipe_id=self.PIPE, filter_date=conditional_filter).get("Data"))
            
            return dados
        except Exception as e:
            logging.info(e)
            raise exceptions(e)
    
    
    def run_created_all_cards(self, data : list) -> NoReturn:
        """
        Função "motor" de chamadas paralelizadas, feita para criar vários cards ao mesmo tempo, de acordo com os dados passados.
        
        Necessário enviar uma lista com todos os campos a serem enviados para cada card, consultar atravez de `Engine().fields` os que são campos `obrigatorio`.
        
        Variavel -> lista:
        [
            {
                "field_id": `ID_DO CAMPO_NO_PIPEFY`, "field_value": `VALOR_ACEITO_PELO_CAMPO` 
            }

        ]
        
        Exemplo:
        [
                
            {
                "field_id": "prioridade", "field_value": "01"
            },
            {
                "field_id": "EMAIL", "field_value": None or Variavel
            }
        ]
        
        """
        try:
            if data:
                with ProcessPoolExecutor() as exe:
                    for f in range(len(data)):
                        fields = data[f]
                        exe.submit(self.create_cards_pipe, fields)
                        
        except Exception as e:
            logging.info(e)
            raise exceptions(e)
        
        
    def run_created_all_cards_phase(self, data : list) -> NoReturn:
        """
        Função "motor" de chamadas paralelizadas, feita para criar vários cards ao mesmo tempo, de acordo com os dados passados.
        
        Necessário enviar uma lista com todos os campos a serem enviados para cada card, consultar atravez de `Engine().fields` os que são campos `obrigatorio`.
        
        Variavel -> lista:
            [
                {
                    "due_date":due_date,
                    "label_ids":label_ids,
                    "phase_id": phase_id,
                    "fields": [
                        {"field_id": "empresa", "field_value": ""},
                        {"field_id": "bancos", "field_value": ""},
                        {"field_id": "data_de_arrecada_o","field_value": ""},
                        {"field_id": "quantidade_recebida", "field_value": ""},
                        {"field_id": "quantidade_baixada", "field_value": ""}
                        {"field_id": "conv_nio", "field_value": None},
                        {"field_id": "ltimo_nsa", "field_value": None},
                        {"field_id": "ltimo_arquivo", "field_value": None},
                        {"field_id": "arquivo", "field_value": None},
                        {"field_id": "e_mail_destinat_rio", "field_value": None},
                        {"field_id": "e_mail_c_pia", "field_value":None}
                    ]
                }
            ]
        
        Exemplo:
        
            cards = [
                {
                    "due_date":due_date,
                    "label_ids":label_ids,
                    "phase_id": phase_id,
                    "fields": [
                        {"field_id": "empresa", "field_value": ""},
                        {"field_id": "bancos", "field_value": ""},
                        {"field_id": "data_de_arrecada_o","field_value": ""},
                        {"field_id": "quantidade_recebida", "field_value": ""},
                        {"field_id": "quantidade_baixada", "field_value": ""}
                        {"field_id": "conv_nio", "field_value": None},
                        {"field_id": "ltimo_nsa", "field_value": None},
                        {"field_id": "ltimo_arquivo", "field_value": None},
                        {"field_id": "arquivo", "field_value": None},
                        {"field_id": "e_mail_destinat_rio", "field_value": None},
                        {"field_id": "e_mail_c_pia", "field_value":None}
                    ]
                }
            ]
        
        """
        try:
            if data:
                with ProcessPoolExecutor() as exe:
                    for f in range(len(data)):
                        n_query = data[f]
                        exe.submit(self.create_cards_pipe_phase, n_query)
                        
        except Exception as e:
            logging.info(e)
            raise exceptions(e)