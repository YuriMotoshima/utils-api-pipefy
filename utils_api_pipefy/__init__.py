"""
utils-api-pipefy — Módulos de integração com API Pipefy GraphQL

Este módulo fornece classes e utilitários para interagir com a API do Pipefy:
- Consulta, criação e atualização de cards, phases, fields, labels, tabelas
- Processamento paralelo via multiprocessing
- Organização e parse de dados retornados pela API

Classes principais:
    - Engine: Orquestrador de operações em massa (run_all_data_phases, run_update_fields_cards)
    - Pipe: Classe base para consultas CRUD de recursos do Pipefy

Exemplo de uso básico:
    ```python
    import os
    from utils_api_pipefy import Engine

    # Carrega variáveis de ambiente (.env)
    eng = Engine()

    # Extrai dados de todas as phases
    data = eng.run_all_data_phases()

    # Atualiza campos em um card específico
    card = eng.card(id=7641824)
    fields = [
        {"fieldId": "texto_longo_vazio", "value": "valor"},
        {"fieldId": "n_guia_no_prestador", "value": "11892332"}
    ]
    eng.update_fields_pipe(card_id=25030802, fields=fields)
    ```

Variáveis de ambiente obrigatórias:
    - TOKEN: Bearer do Pipefy (autenticação GraphQL)
    - HOST_PIPE: app ou host personalizado (padrão: app)
    - PIPE: ID numérico da pipe que será consultada
    - NONPHASES (opcional): IDs de phases a ignorar

Segurança:
    - Logs sanitizados — não expõe dados sensíveis
    - Tokens nunca serializados em logs/arquivos temporários
    - Multiprocessing isolado por processo

Limitações:
    - Processamento paralelo limitado pelo sistema operacional (Unix sockets no Windows)
    - Operações de escrita devem ser idempotentes ou idempotência deve ser implementada pela aplicação
"""

from utils_api_pipefy.libs.engine import Engine
from utils_api_pipefy.libs.excepts import exceptions
from utils_api_pipefy.libs.log import loginit

# Inicializa logging estruturado sanitizado no primeiro uso
loginit()


__version__ = "0.5.3"
__author__ = "Yuri Motoshima <yurimotoshima@gmail.com>"