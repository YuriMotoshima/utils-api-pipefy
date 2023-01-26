# utils-api-pipefy

Biblioteca que possui um kit de ferramentas úteis para ações usualmente rotineiras de quem trabalha com Pipefy, desde consulta de cards a criação de Pipes, Tables e atualizações em geral.

Utilizamos como apoio as collection requests e python-dotenv.

## Instalação

```
pip install utils-api-pipefy
```

## .env
HOST_PIPE=app or seu_host_pipefy<br>
PIPE= seu_numero_pipe<br>
NONPHASES= [numeros_fases_ignoradas]<br>
TOKEN= seu_token<br>
LOGENV = DEV or PROD [ PROD remove urlib3 logs ]<br>
LOGNAME = nome_arquivo_logs<br>
DISABLELOG = True or False [True desabilita a criação de pasta e arquivo de logs, temos essa opção para utilização em plataformas como Google Cloud Platform, neste caso o logging apenas imprime da tela, sem salvar o log.]<br>

## Exemplo de uso

```py
import os
import json
import time
import logging
from utils_api_pipefy import Engine
from utils_api_pipefy import exceptions

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
```