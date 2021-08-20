from pandas.core.frame import DataFrame
from datetime import datetime
import re


class ExceptDuplicate(Exception):
    pass


class DuplicatePipe():
    def __init__(self, data : DataFrame, logger) -> None:
        """Classe criada para tratar duplicidades de um Pipefy."""
        self.data = data
        self.logger = logger
        self.data_unique = self.remove_duplicate_to_data()
        
        
    def remove_duplicate_to_data(self) -> DataFrame:
        """Função que remove a duplicidade do Pipefy de acordo com o campo ['ordem_de_servi_o']."""
        try:
            df = self.data
            df["ordem_de_servi_o"] = df["ordem_de_servi_o"].apply(lambda a: int("".join(re.sub(r"[\D]", "", a))) if a else a )

            df['createdAtData'] = df['createdAt'].apply(lambda a: datetime.strptime(a, '%Y-%m-%dT%H:%M:%S%z').date())
            df['createdAtHora'] = df['createdAt'].apply(lambda a: datetime.strptime(a, '%Y-%m-%dT%H:%M:%S%z').time())
            df.sort_values(by=['id', 'createdAtData', 'createdAtHora'], ascending=True, inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            data = df.loc[:, ["id", "ordem_de_servi_o", "origem", "current_phase", "createdAt", "createdAtData", "createdAtHora"]]
            
            data.drop_duplicates(subset='ordem_de_servi_o', keep='first', inplace=True)
            data.reset_index(drop=True, inplace=True)
            
            return data
        except Exception as e:
            self.logger.info(e)
            raise ExceptDuplicate(e)


    def data_duplicate_all_phase(self) -> list:
        """
        Função que identifica a duplicidade de todo o Pipefy de acordo com a Origem ['origem'].
        `Retorna: list`
        """
        try:
            df = self.data
            origens = list(set(df["origem"])) # <--- Tira a duplicidade da Lista
            json_data = {}
            for origem in origens:
                
                df_o = df.loc[df["origem"] == origem, ["id", "ordem_de_servi_o", "origem", "current_phase", "createdAt", "createdAtData", "createdAtHora"]]
                
                # Separa um dataframe sem duplicidades
                df_os_unicas = self.data_unique.copy()
                df_os_unicas = df_os_unicas.loc[df_os_unicas["origem"] == origem].copy()
                df_os_unicas.sort_values(by=['id'], ascending=True, inplace=True)
                df_os_unicas.reset_index(drop=True, inplace=True)
                
                # Cria um DF com a Diferença
                df_os_duplicadas = df_o.loc[~df_o['id'].isin(df_os_unicas['id'].tolist())]
                df_os_duplicadas = df_os_duplicadas.loc[df_os_duplicadas["origem"] == origem]
                df_os_duplicadas.reset_index(drop=True, inplace=True)
                df_duplicates = [( i, 'OS com Card ativo.', 'Sim') for i in df_os_duplicadas["id"]]
                json_data.update({ origem : df_duplicates })
            
            return json_data
        except Exception as e:
            self.logger.info(e)
            raise ExceptDuplicate(e)
            
            
    def data_duplicate_phase_validation(self) -> list:
        """
        Função que identifica os cards duplicados na fila de Validação.
        `Retorna: list`
        """
        try:
            df = self.data
            df = df.loc[df["origem"] == "CHAMADO", ["id", "ordem_de_servi_o", "origem", "current_phase", "createdAt", "createdAtData", "createdAtHora"]]
            
            # Separa um dataframe sem duplicidades
            df_os_unicas = self.data_unique.copy()
            
            df_os_unicas = df_os_unicas.loc[df_os_unicas["origem"] == "CHAMADO"].copy()
            df_os_unicas.sort_values(by=['id'], ascending=True, inplace=True)
            df_os_unicas.reset_index(drop=True, inplace=True)
            
            # Cria um DF com a Diferença
            df_os_duplicadas = df.loc[~df['id'].isin(df_os_unicas['id'].tolist())]
            df_os_duplicadas = df_os_duplicadas.loc[df_os_duplicadas["origem"] == "CHAMADO"]
            df_os_duplicadas.reset_index(drop=True, inplace=True)
            df_os_duplicadas = df_os_duplicadas.loc[df_os_duplicadas['current_phase'] == 'Validação']
            
            df_os_duplicadas = [ ( i, 'OS com Card ativo.', 'Sim') for i in df_os_duplicadas["id"] ]
            
            # print(df_os_duplicadas)
            
            return df_os_duplicadas
        except Exception as e:
            self.logger.info(e)
            raise ExceptDuplicate(e)    
    
    
    def data_unique_phase_validation(self) -> list:
        """
        Função que identifica os cards novos não duplicados na fila de Validação.
        `Retorna: list`
        """
        try:
            # Separa um dataframe sem duplicidades
            df = self.data_unique.copy()
            
            df_os_unicas = df.loc[df["origem"] == "CHAMADO"].copy()
            df_os_unicas.sort_values(by=['id'], ascending=True, inplace=True)
            df_os_unicas.reset_index(drop=True, inplace=True)
            df_os_unicas = df_os_unicas[df_os_unicas['current_phase'] == 'Validação'].copy()
            
            df_os_unicas =  [ ( i, 'Ok', 'Nao' ) for i in df_os_unicas["id"] ]
            
            # print (df_os_unicas)
            
            return df_os_unicas
        except Exception as e:
            self.logger.info(e)
            raise ExceptDuplicate(e)
        
        
    def run(self) -> list:
        """
        Função `motor` que consolida os cards que precisam ser atualizados no Pipefy.
        `Retorna: list`
        """
        try:
            data = []
            
            data.extend(self.data_duplicate_phase_validation())
            data.extend(self.data_unique_phase_validation())
            
            return data
        except Exception as e:
            self.logger.info(e)
            raise ExceptDuplicate(e)




