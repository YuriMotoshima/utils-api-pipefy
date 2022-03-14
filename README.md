# utils-api-pipefy

Biblioteca que possui um kit de ferramentas úteis para ações usualmente rotineiras de quem trabalha com Pipefy, desde consulta de cards a criação de Pipes, Tables e atualizações em geral.

Utilizamos como apoio as collection requests e python-dotenv.

## Instalação

```
pip install utils-api-pipefy
```

## .env
HOST=app or seu_host_pipefy
PIPE= seu_numero_pipe
NONPHASES= [numeros_fases_ignoradas]
TOKEN= seu_token
LOGENV = DEV or PROD [ PROD remove urlib3 logs ]
LOGNAME = nome_arquivo_logs
DISABLELOG = True or False [False disabilita a criação de pasta e arquivo de logs, temos essa opção para utlização em plataformas como Google Cloud Platform, neste caso o logging apenas imprime da tela, sem salvar o log.]

## Exemplo de uso

```py
import os
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
        print(eng.phase_id)
        print(eng.fields)
        print(eng.phases)
        
        data=eng.run_all_data_phases()
    
    except Exception as err:
        raise exceptions(err)

```