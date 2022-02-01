from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import NoReturn
from utils_api_pipefy.libs.pipefy import Pipefy
from utils_api_pipefy.libs.log import log
import logging

log().loginit()

class PipeExcept(Exception):
    pass


class Pipe(Pipefy):
    def __init__(self, token, host, pipe, nonphases):
        """
            `Classe criada para`:
                - Consumo das API's do Pipefy.
                - Organização dos retornos das API's
                - Padronizar e Estruturar os dados de acordor com os ID dos Fields e Phases
                
            `Herança`:
                - Classe -> Pipefy
                
            `Libs Utilizadas`:
                - Nativas:
                    - from os import getenv
                    - from concurrent.futures import ProcessPoolExecutor, as_completed
                    - from typing import NoReturn
                - Externas:
                    - from dotenv import load_dotenv (Pode ser substituida de acordo com o Usuário)
                    - from api_pipefy.pipefy import Pipefy (Pode alterar de acordo com o Usuário)
                    
            `Variaveis aguardadas`:
                - `TOKEN` : Bearer do Pipefy necessária para autorizar as chamadas da API.
                - `HOST` : localhost necessário para determinar para onde será enviado a chamada da API, por padrão Pipefy é utlizado app.
                - `PIPE` : ID do Pipefy que deverá ser efetuado a query, mutation, etc...
                - `NONPHASE` : Lista com ID das Fases que devem ser ignoradas.
        """
        self.TOKEN = token
        self.HOST = host
        self.PIPE = pipe
        self.NONPHASES = nonphases
        self.phase_id = None
        self.fields = None
        self.phases = None
        self.__get_field_and_phases()
        self.columns = self.get_coluns_id()
    
    
    def _timeit(func):
        """
        Decorator para cronometrar o tempo de exeção das funções.
        """
        def print_time(*args):
            start = datetime.now()
            print(f"{func.__name__} iniciado às {start}.")
            func(*args)
            end = datetime.now() - start
            print(f"{func.__name__} finalizado às {datetime.now()}.\nTempo de execução (hh:mm:ss.ms) {end}")
            
        return print_time
       
        
    def __get_field_and_phases(self) -> NoReturn:
        """
        Função para pegar as fases, campos e id das fases do Pipefy
        
        Chama a API consultaFields e trata o retorno:
            - Cria lista com id das fases.
            - Cria lista com id e nome das fases.
            - Cria lista com id e nome dos campos.
        """
        try:
            super().__init__(token=self.TOKEN, host=self.HOST)
            
            campos_pipefy = self.consultaFields(pipe_id=self.PIPE)
            
            self.phases_id = [n.get("id") for n in campos_pipefy["phases"] if not n.get("id") in self.NONPHASES]
            
            self.fields = {"fields":[{"id": d.get("id"), "nameField":d.get("label"), "editable":d.get("editable"), "uuid":d.get("uuid") , "required":d.get("required")} for n in campos_pipefy['phases'] for d in n['fields']] + [{"id": n.get("id"), "nameField": n.get("label"), "editable":n.get("editable"), "uuid":n.get("uuid"), "required":n.get("required")} for n in campos_pipefy['start_form_fields']]}

            self.phases = {"phases" : [{ "id": d.get("id"), "nameFase": d.get("name"), "informacoes": [ "firstTimeIn", "lastTimeOut"] } for d in campos_pipefy['phases']]}
            
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
    
    
    def __extract_basics(self, card : dict) -> dict:
        """
        Para cada card é pego os dados dos campos abaixo.
        
        Retorna uma lista de dados.
        
        `Exemplo:` ['373380', '209064729', 'Setor - Cliente Final', [], '2021-06-24T06:00:00-03:00', 'Analytics & Innovation Robot', [{...}], None, '2021-06-14T11:32:15-03:00', '2021-07-06T23:26:04-03:00']
        """
        try:
            fields = [card.get("id"),
                    card.get("title"),
                    card.get("current_phase").get("name"),
                    card.get("labels")[0]["name"] if card.get("labels") else None,
                    card.get("due_date"),
                    card.get("createdBy").get("name"),
                    card.get("assignees"),
                    card.get("finished_at"),
                    card.get("createdAt"),
                    card.get("updated_at")
            ]
            return fields
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
        

    def __extract_custom(self, card : dict) -> dict:
        """
        Para cada card, é pego os dados de acordo com os id passados na lista custom_schema
        
        Retorna uma lista de dados.
        
        `Exemplo:` ['14/06/2021 11:30', None, None, None, None, None, None, None, None, None, None, None, None, None, ...]   
        """
        try:
            card_fields = card["fields"]
            return [card_fields.get(n["id"],{}).get("value") for n in self.fields["fields"]]
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
        

    def __extract_phases(self, card : dict) -> dict:
        """
        Para cada card é pego das inicial e final de cada fase no histórico de fases. (card["phases_history"])
        
        Retorna uma lista de dados.
        
        `Exemplo:` ['2021-06-14T14:32:16+00:00', '2021-06-14T14:32:18+00:00', None, None, None, None, None, None, '2021-07-07T02:26:04+00:00', None, None, None, None, None, ...]
        """
        try:
            fields = []
            phases_history = card["phases_history"]
            for phase in self.phases["phases"]:
                dados_phase = phases_history.get(phase["id"])
                informacoes = phase["informacoes"]
                if dados_phase:
                    for info in informacoes:
                        fields.append(dados_phase[info])
                else:
                    [fields.append(None) for x in informacoes]
            return fields
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
        
        
    def parse_phase_history(self, card : dict) -> dict:
        """
        Para cada card é tratado o campo phases_history:
            - Organiza por ID da Fase
                * Pegando Data de Inicio, Data Final, Duração e Data de Criação do card na Fase
        Retorna o mesmo card com atualização do campo [phases_history], formatado como descrito acima. De acordo com o ID da Fase Histórica. 
        
        `Exemplo:` {'401': {'firstTimeIn': '2021-06-14T14:32:15+00:00', 'lastTimeOut': '2021-06-14T14:32:16+00:00', 'duration': 0, 'created_at': '2021-06-14T11:32:15-03:00'}
        """
        try:
            history = {d["phase"]["id"]: {"firstTimeIn":d["firstTimeIn"],"lastTimeOut":d["lastTimeOut"],"duration": d["duration"], "created_at": d["created_at"]} for d in card["phases_history"]}
            card["phases_history"] = history
            return card
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
        
        
    def get_fields(self, card : dict) -> dict:
        """
        Para cada card é tratado o campo fields:
            - Organiza por ID do cada field dentro de fields:
                * Pegando o Valor, Array de Valor e Data de cada campo
                
        Retorna o mesmo card com atualização do campo [fields], formatado como descrito acima. 
        
        `Exemplo:` 'i_equipamento': {'value': 'NÃO', 'array_value': None, 'datetime_value': None}
        """
        try:
            new_fields = { item["field"]["id"] : {"value": item.get("value"), "array_value": item.get("array_value"), "datetime_value": item.get("dt_time_value")} for item in card["fields"] }
            card["fields"] = new_fields
            return card
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
            
            
    def parse_card(self, card : list) -> tuple:
        """
        Função para organizar e pega os dados ordenadamente de cada card, de acordo com os campos do json de retorno do Pipefy.
        
        Retorna uma tupla com os dados de cada card
        
        `Exemplo:` ('111111', '2222222222', 'Setor - Cliente Final', [...], '2021-06-24T06:00:00-03:00', 'Analytics & Innovation Robot', [...], None, '2021-06-14T11:32:15-03:00', ...),
        
        ('id', 'title', 'current_phase', 'labels', 'due_date', 'createdBy', 'assignees', 'finished_at', 'createdAt', 'updated_at', 'sla', 'dados_ok', 'mensagem_de_duplicidade', 'os_procedente_1', ...)
        """
        try:
            basic_fields = self.__extract_basics(card)
            custom_fields = self.__extract_custom(card)
            phases_fields = self.__extract_phases(card)

            all_fields = basic_fields + custom_fields + phases_fields

            return tuple(all_fields)
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
        
        
    def get_coluns_id(self) -> tuple:
        """
        Função para organizar e pegar os id dos campos ordenadamente de acordo com o json de retorno do Pipefy.
        
        Retorna uma tupla com os id dos campos
                
        `Exemplo:` ('id', 'title', 'current_phase', 'labels', 'due_date', 'createdBy', 'assignees', 'finished_at', 'createdAt', 'updated_at', 'sla', 'dados_ok', 'mensagem_de_duplicidade', 'os_procedente_1', ...)
        """
        try:
            basic_cols = ["id","title","current_phase","labels","due_date","createdBy","assignees","finished_at","createdAt","updated_at"]
            
            custom_cols = [col["id"] for col in self.fields["fields"] ]
            phases_cols = []
            for phase in self.phases["phases"]:
                phase_name = phase["id"]

                # cadastra nome para cada coluna informada
                for info in phase["informacoes"]:
                    if info == "firstTimeIn":
                        col_name = "fti_" + phase_name
                    elif info == "lastTimeOut":
                        col_name = "lto_" + phase_name
                    elif info == "created_at":
                        col_name = "created_at_" + phase_name
                    elif info == "duration":
                        col_name = "total_time_in_" + phase_name
                    phases_cols.append(col_name)
                    
            all_cols = basic_cols + custom_cols + phases_cols

            return tuple(all_cols)
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
          
            
    def parse_data_cards(self, response_dados : list) -> list:
        """
        Recebe uma lista de dados da chamada do Pipefy e trata:
            - Extrai os dados do array de histórico para cada card.
            - Extrai os dados de cada campo para cada card.
            - Ordena e consolida as extrações de dados.
            
        Retorna uma tupla com os dados organizados.
        
        `Exemplo:` ('111111', '3 - 2222222222222', 'Backlog', None, None, 'Analytics & Innovation Robot', [], None, '2021-08-10T14:53:06-03:00', '2021-08-10T14:53:18-03:00', None, None, None, None, ...)
        """
        try:
            trata_historico = [ self.parse_phase_history(card) for card in response_dados ]
            
            trata_campos = [ self.get_fields(card) for card in trata_historico ]
            
            dados_do_cards = [ self.parse_card(card) for card in trata_campos ]
            
            return dados_do_cards
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
    
    
    def get_data_phase(self, phase_id : str) -> dict:
        """
        Função que pega todos os cards da phase selecionada fasendo a paginação de acordo com a tag (pageInfo) retornada pelo Pipefy.
        Utiliza a função (parse_data_cards) para organizar os dados recebidos.
        
        Retorna um dicionario contendo Status : boolean e Data : list.
        
        `Exemplo:` {"Status":True,"Data": [(....)(....)]}
        """
        try:
            response_phase = self.phase(id=phase_id)
            
            next_page_phase = response_phase['cards']['pageInfo']['hasNextPage']
            
            response_edges_phase = response_phase['cards'].get('edges', {})
            
            if response_edges_phase:
                
                response_dados = [n["node"] for n in response_edges_phase if response_edges_phase]
                
                dados_cards = self.parse_data_cards(response_dados=response_dados)
                
                while next_page_phase:
                    
                    super().__init__(token=self.TOKEN, host=self.HOST)
                    
                    after = response_phase['cards']['pageInfo']['endCursor'] if response_phase['cards']['pageInfo']['hasNextPage'] else None
                    
                    response_phase = self.phase(id=phase_id, after=after)
                    
                    next_page_phase = response_phase['cards']['pageInfo']['hasNextPage']
            
                    response_edges_phase = response_phase['cards'].get('edges', {})
                    
                    response_dados = [n["node"] for n in response_edges_phase]
                    
                    dados_cards += self.parse_data_cards(response_dados=response_dados)
                    
                return {"Status":True,"Data": dados_cards}
            else:
                return {"Status":False,"Data": None}

        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
        
    
    def update_fields_pipe(self, card_id : str, fields : dict) -> NoReturn:
        """
        Função de atualização de campos do Pipefy - API updateFieldsCard.
        """
        try:
            
            response_fields = ', '.join(['{fieldId: "%s", value: "%s"}' % (key, fields[key]) for key in fields])
            
            super().__init__(token=self.TOKEN, host=self.HOST)
            response = self.updateFieldsCard(nodeId=card_id, response_fields=response_fields)
            logging.info(f"Response: {response}")
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
    
    
    def create_cards_pipe(self, fields : dict) -> NoReturn:
        """
        Função de atualização de campos do Pipefy - API updateFieldsCard.
        """
        try:
            
            super().__init__(token=self.TOKEN, host=self.HOST)
            response = self.createCard(pipe_id=self.PIPE, fields_attributes=fields)
            logging.info(f"Response: {response}")
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
    
    
    def create_cards_pipe_phase(self, fields : dict) -> NoReturn:
        """
        Função de atualização de campos do Pipefy - API updateFieldsCard.
        """
        try:
            
            super().__init__(token=self.TOKEN, host=self.HOST)
            response = self.createCardPhase( pipe_id=self.PIPE, fields_attributes=fields.get("fields")[0], due_date=fields.get("due_date"), phase_id=fields.get("phase_id"), label_ids=[fields.get("label_ids")] )
            logging.info(f"Response: {response}")
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
    
    def delete_cards(self, card_id : str) -> dict:
        """
        Função que chama API do Pipefy para deletar cards.
        """
        try:
            super().__init__(token=self.TOKEN, host=self.HOST)
            response = self.deleteCard(id=card_id)
            logging.info(f"Exclusão de Cards - Card_ID: {card_id} - Response: {response}.")
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)


    def change_properties_fields(self, data : str ) -> NoReturn:
        """
        Função que chama API do Pipefy que altera as propriedades de um campo.
        """
        try:
            super().__init__(token=self.TOKEN, host=self.HOST)
            response = self.updatePropertiesFields(fields_attributes=data)
            logging.info(f"Response: {response}.")
        except Exception as e:
            logging.info(e)
            raise PipeExcept(e)
        
        
    def get_data_cards_filter(self, pipe_id, filter_date : str = None) -> dict:
        """
        Função que pega todos os cards do Pipefy selecionado fazendo a paginação de acordo com a tag (pageInfo) retornada pelo Pipefy.
        Utiliza a função (parse_data_cards) para organizar os dados recebidos.
        
        Retorna um dicionario contendo Status : boolean e Data : list.
        
        `Exemplo:` {"Status":True,"Data": [(....)(....)]}
        """
        filter_date = filter_date or None
        
        try:
            response_phase = self.allCards(pipe_id=pipe_id, filter_date=filter_date)
            
            next_page_phase = response_phase['pageInfo']['hasNextPage']
            
            response_edges_phase = response_phase.get('edges', {})
            
            if response_edges_phase:
                
                response_dados = [n["node"] for n in response_edges_phase if response_edges_phase]
                
                dados_cards = self.parse_data_cards(response_dados=response_dados)
                
                while next_page_phase:
                    
                    super().__init__(token=self.TOKEN, host=self.HOST)
                    
                    after = response_phase['pageInfo']['endCursor'] if response_phase['pageInfo']['hasNextPage'] else None
                    
                    response_phase = self.allCards(pipe_id=pipe_id, after=after, filter_date=filter_date)
                    
                    next_page_phase = response_phase['pageInfo']['hasNextPage']
            
                    response_edges_phase = response_phase.get('edges', {})
                    
                    response_dados = [n["node"] for n in response_edges_phase]
                    
                    dados_cards += self.parse_data_cards(response_dados=response_dados)
                
                return {"Status":True,"Data": dados_cards}
            else:
                return {"Status":False,"Data": "None"}

        except Exception as e:
            self.logger.info(e)
            raise PipeExcept(e)