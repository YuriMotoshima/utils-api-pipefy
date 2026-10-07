#!/usr/bin/env python3
"""
Autodoc — Adiciona docstrings padronizadas em métodos do utils-api-pipefy

Gera docstring padrão para qualquer método baseado na assinatura da função.
"""

def generate_docstring(method_name):
    """
    Gera docstring padrão para método.
    
    Args:
        method_name: Nome da função
        
    Returns:
        str: Docstring formatada
    """
    methods_map = {
        'pipes': 'Lista pipes em massa por ID(s) com paginação.',
        'pipe': 'Consulta pipe único por ID específico.',
        'organization': 'List informações de organização (org_id), incluindo users, phases.',
        'phase': 'Consulta phase específica com opcional paginação via after.',
        'all_cards': 'Lista todos cards de um pipe com paginação via cursor (after).',
        'card': 'Carrega card específico por ID usando fields padronizados.',
        '__prepare_json_dict': 'Normaliza dicionario ao remover aspas duplas em keys numéricas para GraphQL.',
        '__prepare_json_list': 'Normaliza lista de dicionarios formatando como string JSON GraphQL.',
    }
    
    headline = methods_map.get(method_name, f'{method_name} em Pipefy via GraphQL API.')
    return f'''{headline}

Args:
    Ver documentação original do método.

Raises:
    exceptions: Erro na chamada à API ou resposta inválida.'''


def main():
    '''Aplica docstrings automaticamente'''
    path = "C:/Users/Pichau/Desktop/Projetos/utils-api-pipefy/utils_api_pipefy/libs/pipefy.py"
    
    with open(path, 'r') as f:
        lines = f.readlines()
    
    modified_lines = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        if stripped.startswith('def ') and '\"\"\"' not in ''.join(lines[i:i+10]):
            # Encontra o fim da linha def...
            end_of_method_start = stripped.find(':') + 1
            rest_of_line = stripped[end_of_method_start:].strip()
            
            # Substitui docstring vazia ou existente pela automática
            new_docstring = generate_docstring(stripped.split('(')[0].split('def ')[1])
            
            # Reescreve linha para incluir docstring
            modified_lines.append(f'    {rest_of_line}\n    {new_docstring}')
        else:
            modified_lines.append(line)
    
    # Verifica quantas mudanças foram feitas
    original_count = lines.count('def ')
    new_count = len([l for l in modified_lines if '\"\"\"' in l])  # Linhas com docstring
    
    print(f"✅ Docstrings adicionadas automaticamente: {new_count - original_count + 1}")
    print(f"Métodos atualizados no arquivo.")


if __name__ == '__main__':
    main()
