from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime
from typing import NoReturn
from libs.utilits_pipe import Pipe

class EngineExcept(Exception):
    pass

class Engine(Pipe):
    def __init__(self, token, host, pipe, nonphases, logger):
        super().__init__(token, host, pipe, nonphases, logger)

    def _timeit(func):
        def print_time(*args):
            start = datetime.now()
            print(f"{func.__name__} iniciado às {start}.")
            func(*args)
            end = datetime.now() - start
            print(f"{func.__name__} finalizado às {datetime.now()}.\nTempo de execução (hh:mm:ss.ms) {end}")
            
        return print_time
    
    
    @_timeit
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
                    self.logger.info(f"\nPhase: {phase}, Worker Running: {worker.running()}.")
                    
                for worker in as_completed(list_future):
                    if worker.result().get("Status"):
                        dados.extend(worker.result().get("Data"))
            
            return dados
        except Exception as e:
            self.logger.info(e)
            raise EngineExcept(e)
    

        


    def run_update_phase_validation(self, data : dict, automatic_editable : str = None) -> NoReturn:
        """
        Função "motor" de chamadas paralelizadas, feita para atualizar campos de vários cards ao mesmo tempo, de acordo com os dados passadas.
        
        Enviar em `data`:
        
            - card_id <== Número do card a ser atualizado
            - fields <== [{}] Lista com dicionário informando o ID do field e Valor a ser preenchido.
            
        Exemplo `data`:
            {
                "card_id":"12132131",
                "fields":[
                    {
                        "mensagem_de_duplicidade":"Card duplicado",
                        "dados_ok":"Não",
                        "title":"Card Improcedente"
                    }
                ]
            }
        
        """
        try:
            if data:
                with ProcessPoolExecutor() as exe:
                    for d in data:
                        card_id = data.get("card_id")
                        fields = data.get("fields")
                        exe.submit(self.update_fields_pipe, card_id, fields)
                        
        except Exception as e:
            self.logger.info(e)
            raise EngineExcept(e)
    
     
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
                        self.logger.info(f"\nCard_ID: {card[0]}, Worker Running: {worker.running()}.")
                        
            elif msg == 0:
                self.logger.info(f"Operação cancelada !")
                
            else:
                self.logger.info(f"Opção incorreta, processo cancelado!")
                                  
        except Exception as e:
            self.logger.info(e)
            raise  EngineExcept(e)
