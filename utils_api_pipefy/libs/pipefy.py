# -*- coding: utf-8 -*-
import json
import re
import time

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils_api_pipefy.libs.excepts import exceptions

urllib3.disable_warnings()


class Pipefy(object):
    """ Integration class with Pipefy rest api. """

    def __init__(self, token, host="app"):
        self.token = token if 'Bearer' in token else 'Bearer %s' % token
        self.headers = {'Content-Type': 'application/json', 'Authorization': self.token}
        self.endpoint = f'https://{host}.pipefy.com/graphql'


    def request(self, query, headers={}, schema : str = "query"):
      
      def get_request(headers:dict, query:str):
        session_request = requests.Session()
        retry = Retry(total=5, backoff_factor=45)
        adapter = HTTPAdapter(max_retries=retry)
        session_request.mount("https://", adapter)
        
        resp = session_request.post( self.endpoint, json={ schema : query }, headers=headers, verify=False)
        return resp
      
      _headers = self.headers
      _headers.update(headers)
      
      response = get_request(headers=_headers, query=query)
      count_try = 0
      status_code = int(response.status_code)
      
      while count_try < 4 and status_code > 399:
        time.sleep(40)
        response = get_request(headers=_headers, query=query)
        count_try = count_try + 1
        status_code = int(response.status_code)
        print(f"{count_try} connection attempt made. Status Code: {status_code}")
        
      try:
          response = json.loads(response.text)
      except ValueError:
          raise exceptions(response.text)

      if response.get('error'):
          raise exceptions(response.get('error_description', response.get('error')))
        
      if response.get('errors'):
          for error in response.get('errors'):
              raise exceptions(error.get('message'))
      return response


    def __prepare_json_dict(self, data_dict):
        data_response = json.dumps(data_dict)
        rex = re.compile(r'"(\S+)":')
        for field in rex.findall(data_response):
            data_response = data_response.replace('"%s"' % field, field)
        return data_response


    def __prepare_json_list(self, data_list):
      try:
        return '[ %s ]' % ', '.join([self.__prepare_json_dict(data) for data in data_list])
      except Exception as err:
        raise exceptions(err)

    def pipes(self, ids=[], response_fields=None, headers={}):
        """ List pipes: Get pipes by their identifiers. """
        try:

          response_fields = response_fields or 'id name phases { name cards (first: 5)' \
                                                      ' { edges { node { id title } } } }'
          query = '{ pipes (ids: [%(ids)s]) { %(response_fields)s } }' % {
              'ids': ', '.join([json.dumps(id) for id in ids]),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('pipes', [])
        
        except Exception as err:
          raise exceptions(err)

      
    def organization(self, org_id, response_fields=None, headers={}):
        """ List fields: Get organization by their identifiers. """
        try:

          response_fields = response_fields or 'id name users { id name email username departmentKey }  '
          
          query = '{ organization (id: %(org_id)s) { %(response_fields)s } }' % {
              'org_id': json.dumps(org_id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('organization', [])
        
        except Exception as err:
          raise exceptions(err)
        
        
    def pipe(self, id, response_fields=None, headers={}):
        """ Show pipe: Get a pipe by its identifier. """
        try:
          response_fields = response_fields or 'id name start_form_fields { label id }' \
                                                      ' labels { name id } phases { name fields { label id }' \
                                                      ' cards(first: 5) { edges { node { id, title } } } }'
          query = '{ pipe (id: %(id)s) { %(response_fields)s } }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('pipe', [])
        
        except Exception as err:
          raise exceptions(err)
        

    def clone_pipes(self, organization_id, pipe_template_ids=[], response_fields=None, headers={}):
        """ Clone pipe: Mutation to clone a pipe, in case of success a query is returned. """
        try:
          
          response_fields = response_fields or 'pipes { id name }'
          query = 'mutation { clonePipes(input: { organization_id: %(organization_id)s' \
                      ' pipe_template_ids: [%(pipe_template_ids)s] }) { %(response_fields)s } }' % {
              'organization_id': json.dumps(organization_id),
              'pipe_template_ids': ', '.join([json.dumps(id) for id in pipe_template_ids]),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('clonePipes', {}).get('pipe', [])
        
        except Exception as err:
          raise exceptions(err)

    def create_pipe(self, organization_id, name, labels=[], members=[], phases=[],
                            start_form_fields=[], preferences={},  response_fields=None, headers={}):
        """ Create pipe: Mutation to create a pipe, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'pipe { id name }'
          query = '''
              mutation {
                createPipe(
                  input: {
                    organization_id: %(organization_id)s
                    name: %(name)s
                    labels: %(labels)s
                    members: %(members)s
                    phases: %(phases)s
                    start_form_fields: %(start_form_fields)s
                    preferences: %(preferences)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'organization_id': json.dumps(organization_id),
              'name': json.dumps(name),
              'labels': self.__prepare_json_list(labels),
              'members': self.__prepare_json_list(members),
              'phases': self.__prepare_json_list(phases),
              'start_form_fields': self.__prepare_json_list(start_form_fields),
              'preferences': self.__prepare_json_dict(preferences),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createPipe', {}).get('pipe', [])
        
        except Exception as err:
          raise exceptions(err)

    def update_pipe(self, id, icon=None, title_field_id=None, public=None, public_form=None,
                        only_assignees_can_edit_cards=None,  anyone_can_create_card=None,
                        expiration_time_by_unit=None, expiration_unit=None, response_fields=None, headers={}):
        """ Update pipe: Mutation to update a pipe, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'pipe { id name }'
          query = '''
              mutation {
                updatePipe(
                  input: {
                    id: %(id)s
                    icon: %(icon)s
                    title_field_id: %(title_field_id)s
                    public: %(public)s
                    public_form: %(public_form)s
                    only_assignees_can_edit_cards: %(only_assignees_can_edit_cards)s
                    anyone_can_create_card: %(anyone_can_create_card)s
                    expiration_time_by_unit: %(expiration_time_by_unit)s
                    expiration_unit: %(expiration_unit)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'id': json.dumps(id),
              'icon': json.dumps(icon),
              'title_field_id': json.dumps(title_field_id),
              'public': json.dumps(public),
              'public_form': json.dumps(public_form),
              'only_assignees_can_edit_cards': json.dumps(only_assignees_can_edit_cards),
              'anyone_can_create_card': json.dumps(anyone_can_create_card),
              'expiration_time_by_unit': json.dumps(expiration_time_by_unit),
              'expiration_unit': json.dumps(expiration_unit),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updatePipe', {}).get('pipe', [])

        except Exception as err:
          raise exceptions(err)

    def delete_pipe(self, id, response_fields=None, headers={}):
        """ Delete pipe: Mutation to delete a pipe, in case of success success: true is returned. """
        try:
          
          response_fields = response_fields or 'success'
          query = 'mutation { deletePipe(input: { id: %(id)s }) { %(response_fields)s }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deletePipe', {})
        
        except Exception as err:
          raise exceptions(err)

    def phase(self, id, after = None, response_fields=None, headers={}):
        """ Show phase: Get a phase by its identifier. """
        try:
          if after:
            response_fields = response_fields or 'pageInfo { endCursor hasNextPage } edges { node { id title createdAt finished_at updated_at createdBy { id name } assignees { id name email } comments { text } comments_count current_phase { name } done due_date fields { name value datetime_value field { id } array_value  } labels { name } createdAt phases_history { phase { name id } created_at duration firstTimeIn lastTimeOut } url } }'
            query = '{ phase(id: %(id)s){ cards_count cards(after:%(after)s) {%(response_fields)s } } }' % {
                'id': json.dumps(id),
                'after':json.dumps(after),
                'response_fields': response_fields,
            }
            return self.request(query, headers).get('data', {}).get('phase')
          else:
            response_fields = response_fields or 'cards { pageInfo { endCursor hasNextPage } edges { node { id title finished_at updated_at createdBy { id name } assignees { id name email } comments { text } comments_count current_phase { name } done due_date fields { name value datetime_value field { id } array_value } labels { name } createdAt phases_history { phase { name id } created_at duration firstTimeIn lastTimeOut } url } } }'
            query = '{ phase(id: %(id)s) { cards_count  %(response_fields)s} }' % {
                'id': json.dumps(id),
                'response_fields': response_fields,
            }
            return self.request(query, headers).get('data', {}).get('phase')
        
        except Exception as err:
          raise exceptions(err)

    def find_card(self, pipe_id:str, field_id: str, field_value:str, after = None, response_fields=None, headers={}):
        """ Show phase: Get a phase by its identifier. """
        try:
          response_fields = response_fields or 'edges { node { id title finished_at updated_at createdBy { id name } assignees { id name email } comments { text } comments_count current_phase { name } done due_date fields { name value datetime_value field { id } array_value } labels { name } createdAt phases_history { phase { name id } created_at duration firstTimeIn lastTimeOut } url } } pageInfo { endCursor hasNextPage }'
          if after:
            query = '{ findCards(pipeId: %(pipe_id)s, after: %(after)s , search: {fieldId: %(field_id)s, ' \
                    'fieldValue: %(field_value)s}) {%(response_card)s }}' \
                    % {
                        "pipe_id": json.dumps(pipe_id),
                        "field_id": json.dumps(field_id),
                        "after": json.dumps(after),
                        "field_value": json.dumps(field_value),
                        "response_card": response_fields
                    }
            return self.request(query, headers).get('data', {}).get('findCards')
          
          else:
            query = '{findCards(pipeId: %(pipe_id)s, search: {fieldId: %(field_id)s, ' \
                    'fieldValue: %(field_value)s}) {%(response_card)s}}' \
                    % {
                        "pipe_id": json.dumps(pipe_id),
                        "field_id": json.dumps(field_id),
                        "field_value": json.dumps(field_value),
                        "response_card": response_fields
                    }
            return self.request(query, headers).get('data', {}).get('findCards')
        except Exception as err:
          raise exceptions(err)

    def create_phase(self, pipe_id, name, done, lateness_time, description, can_receive_card_directly_from_draft,
                            response_fields=None, headers={}):
        """ Create phase: Mutation to create a phase, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'phase { id name }'
          query = '''
              mutation {
                createPhase(
                  input: {
                    pipe_id: %(pipe_id)s
                    name: %(name)s
                    done: %(done)s
                    lateness_time: %(lateness_time)s
                    description: %(description)s
                    can_receive_card_directly_from_draft: %(can_receive_card_directly_from_draft)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'pipe_id': json.dumps(pipe_id),
              'name': json.dumps(name),
              'done': json.dumps(done),
              'lateness_time': json.dumps(lateness_time),
              'description': json.dumps(description),
              'can_receive_card_directly_from_draft': json.dumps(can_receive_card_directly_from_draft),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createPhase', {}).get('phase')
      
        except Exception as err:
            raise exceptions(err)


    def update_phase(self, id, name, done, description, can_receive_card_directly_from_draft,
                            response_fields=None, headers={}):
        """ Update phase: Mutation to update a phase, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'phase { id name }'
          query = '''te
              mutation {
                updatePhase(
                  input: {
                    id: %(id)s
                    name: %(name)s
                    done: %(done)s
                    description: %(description)s
                    can_receive_card_directly_from_draft: %(can_receive_card_directly_from_draft)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'id': json.dumps(id),
              'name': json.dumps(name),
              'done': json.dumps(done),
              # 'lateness_time': json.dumps(lateness_time),
              'description': json.dumps(description),
              'can_receive_card_directly_from_draft': json.dumps(can_receive_card_directly_from_draft),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updatePhase', {}).get('phase')
        
        except Exception as err:
          raise exceptions(err)

    def delete_phase(self, id, response_fields=None, headers={}):
        """ Delete phase: Mutation to delete a phase of a pipe, in case of success success: true is returned. """
        try:
          
          response_fields = response_fields or 'success'
          query = 'mutation { deletePhase(input: { id: %(id)s }) { %(response_fields)s }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deletePhase', {})
        
        except Exception as err:
          raise exceptions(err)


    def create_phase_field(self, phase_id, type, label, options, description, required, editable,
                            response_fields=None, headers={}):
        """ Create phase field: Mutation to create a phase field, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'phase_field { id label }'
          query = '''
              mutation {
                createPhaseField(
                  input: {
                    phase_id: %(phase_id)s
                    type: %(type)s
                    label: %(label)s
                    options: %(options)s
                    description: %(description)s
                    required: %(required)s
                    editable: %(editable)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'phase_id': json.dumps(phase_id),
              'type': json.dumps(type),
              'label': json.dumps(label),
              'options': self.__prepare_json_list(options),
              'description': json.dumps(description),
              'required': json.dumps(required),
              'editable': json.dumps(editable),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createPhaseField', {}).get('phase_field')
        
        except Exception as err:
          raise exceptions(err)


    def update_phase_field(self, id, label, options, required, editable, response_fields=None, headers={}):
        """ Update phase field: Mutation to update a phase field, in case of success a query is returned. """
        try:
          
          response_fields = response_fields or 'phase_field { id label }'
          query = '''
              mutation {
                updatePhaseField(
                  input: {
                    id: %(id)s
                    label: %(label)s
                    options: %(options)s
                    required: %(required)s
                    editable: %(editable)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'id': json.dumps(id),
              'label': json.dumps(label),
              'options': self.__prepare_json_list(options),
              'required': json.dumps(required),
              'editable': json.dumps(editable),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updatePhaseField', {}).get('phase_field')
        
        except Exception as err:
          raise exceptions(err)


    def delete_phase_field(self, id, response_fields=None, headers={}):
        """ Delete phase field: Mutation to delete a phase field, in case of success success: true is returned. """
        try:
          
          response_fields = response_fields or 'success'
          query = 'mutation { deletePhaseField(input: { id: %(id)s }) { %(response_fields)s }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deletePhaseField', {})
        
        except Exception as err:
          raise exceptions(err)


    def create_label(self, pipe_id, name, color, response_fields=None, headers={}):
        """ Create label: Mutation to create a label, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'label { id name }'
          query = '''
              mutation {
                createLabel(
                  input: {
                    pipe_id: %(pipe_id)s
                    name: %(name)s
                    color: %(color)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'pipe_id': json.dumps(pipe_id),
              'name': json.dumps(name),
              'color': json.dumps(color),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createLabel', {}).get('label')
        
        except Exception as err:
          raise exceptions(err)


    def update_label(self, id, name, color, response_fields=None, headers={}):
        """ Update label: Mutation to update a label, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'label { id name }'
          query = '''
              mutation {
                updateLabel(
                  input: {
                    id: %(id)s
                    name: %(name)s
                    color: %(color)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'id': json.dumps(id),
              'name': json.dumps(name),
              'color': json.dumps(color),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updateLabel', {}).get('label')
        
        except Exception as err:
          raise exceptions(err)


    def delete_label(self, id, response_fields=None, headers={}):
        """ Delete label: Mutation to delete a label, in case of success success: true is returned. """
        try:
          
          response_fields = response_fields or 'success'
          query = 'mutation { deleteLabel(input: { id: %(id)s }) { %(response_fields)s }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deleteLabel', {})
        
        except Exception as err:
          raise exceptions(err)

    def cards(self, pipe_id, count=10, search={}, response_fields=None, headers={}):
        """ List cards: Get cards by pipe identifier. """
        try:
          
          response_fields = response_fields or 'edges { node { id title createdAt assignees { id }' \
                  ' comments { text } comments_count current_phase { name } done due_date ' \
                  'fields { field { id uuid } name value } labels { name } phases_history { phase { name } firstTimeIn lastTimeOut } url } }'
                  
          query = '{ cards(pipe_id: %(pipe_id)s, first: %(count)s, search: %(search)s) { %(response_fields)s } }' % {
              'pipe_id': json.dumps(pipe_id),
              'count': json.dumps(count),
              'search': self.__prepare_json_dict(search),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('cards', [])
        
        except Exception as err:
          raise exceptions(err)

    def consulta_fields(self, pipe_id, response_fields=None, headers={}):
      """ List fiels: Get fields by pipe identifier. """
      try:
        
        response_fields = response_fields or ' organization { id } cards_count phases { id name fields { id label editable uuid required} } start_form_fields { id label editable uuid required } '
        query = '{ pipe(id:%(pipe_id)s) { %(response_fields)s } }' % {
          'pipe_id': json.dumps(pipe_id),
          'response_fields': response_fields
        }
        return self.request(query, headers).get('data', {}).get('pipe', [])
      
      except Exception as err:
          raise exceptions(err)
    
    def update_properties_fields(self, id = None, label = None, editable = None, uuid = None, fields_attributes=None, headers={}):
      """ Mutation: Change Name and Editable to Fields. """
      try:
        
        if fields_attributes:
          update = [fr"{n}" for n in fields_attributes]
          query = ''
          for index, fields_attributes in enumerate(update):
            
            query += 'V%(index)s : updatePhaseField(input:%(fields_attributes)s) { phase_field{   id   label  editable uuid } } , ' % {
              'index': index,
              'fields_attributes': fields_attributes
            }
            
          query = 'mutation {%(query)s}' % { 'query' : query }
          return self.request(query, headers).get('data', {})
        elif id and label and editable and uuid:
          fields_attributes = '{id: "%s", label: "%s", editable:  %s, uuid:  "%s" }' % (id, label, editable, uuid)
          
          query = '{ updatePhaseField(input:%(fields_attributes)s) { phase_field{   id   label   editable } } }' % {
            'fields_attributes': fields_attributes
          }
          return self.request(query, headers, schema="mutation").get('data', {})

      except Exception as err:
          raise exceptions(err)
    
    def all_cards(self, pipe_id, after=None, filter_date = None, response_fields=None, headers={}):
        """ List cards: Get cards by pipe identifier. """
        try:
          response_fields = response_fields or 'pageInfo { endCursor hasNextPage } edges { node { id title finished_at updated_at createdBy { id name } assignees { id name email } comments { text } comments_count current_phase { name } done due_date fields { name value datetime_value field { id } array_value  } labels { name } createdAt phases_history { phase { name id } created_at duration firstTimeIn lastTimeOut } url } }'
          if after:
              if filter_date:
                  query = '{ allCards(pipeId: %(pipe_id)s, after: %(after)s, %(filter_date)s) { %(response_fields)s } }' % {
                  'pipe_id': json.dumps(pipe_id),
                  'after' : json.dumps(after),
                  'filter_date': filter_date,
                  'response_fields': response_fields,
              }
              else:
                  query = '{ allCards(pipeId: %(pipe_id)s, after: %(after)s) { %(response_fields)s } }' % {
                  'pipe_id': json.dumps(pipe_id),
                  'after' : json.dumps(after),
                  'response_fields': response_fields,
              }
              return self.request(query, headers).get('data', {}).get('allCards', [])
          else:
              if filter_date:
                  query = '{ allCards(pipeId: %(pipe_id)s, %(filter_date)s) { %(response_fields)s } }' % {
                      'pipe_id': json.dumps(pipe_id),
                      'filter_date': filter_date,
                      'response_fields': response_fields,
                  }
              else:
                  query = '{ allCards(pipeId: %(pipe_id)s) { %(response_fields)s } }' % {
                      'pipe_id': json.dumps(pipe_id),
                      'response_fields': response_fields,
                  }
              return self.request(query, headers).get('data', {}).get('allCards', [])
        except Exception as err:
          raise exceptions(err)  
            

    def card(self, id, response_fields=None, headers={}):
        """ Show card: Get a card by its identifier. """
        try:
          
          response_fields = response_fields or 'title createdAt assignees { id } comments { id } comments_count' \
                  ' current_phase { name } done due_date fields { field { id uuid } name value } labels { name } phases_history ' \
                  '{ phase { name } firstTimeIn lastTimeOut } url '
          query = '{ card(id: %(id)s) { %(response_fields)s } }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('card', [])
        
        except Exception as err:
          raise exceptions(err)


    def create_card(self, pipe_id, fields_attributes, parent_ids=[], response_fields=None, headers={}):
        """ Create card: Mutation to create a card, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'card { id title }'
          query = '''
              mutation {
                createCard(
                  input: {
                    pipe_id: %(pipe_id)s
                    fields_attributes: %(fields_attributes)s
                    parent_ids: [ %(parent_ids)s ]
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'pipe_id': json.dumps(pipe_id),
              'fields_attributes': self.__prepare_json_dict(fields_attributes),
              'parent_ids': ', '.join([json.dumps(id) for id in parent_ids]),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createCard', {}).get('card')
        except Exception as err:
          raise exceptions(err)


    def update_card(self, id, title=None, due_date=None, assignee_ids=[], label_ids=[], response_fields=None, headers={}):
        """ Update card: Mutation to update a card, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'card { id title }'
          str_due_date = due_date.strftime('%Y-%m-%dT%H:%M:%S+00:00') if due_date else json.dumps(due_date)
          str_assignees = ', '.join([json.dumps(id) for id in assignee_ids])
          str_label_ids = ', '.join([json.dumps(id) for id in label_ids])
          query = '''
              mutation {
              updateCard(
                  input: {
                  %(id)s
                  %(title)s
                  %(due_date)s
                  %(assignee_ids)s
                  %(label_ids)s
                  }
              ) { %(response_fields)s }
              }
          ''' % {
              'id': f'id:{json.dumps(id)}' if id else '',
              'title': f'title:{json.dumps(title)}' if title else '',
              'due_date': f'due_date: {str_due_date}' if due_date else '',
              'assignee_ids': f'assignee_ids: {str_assignees}' if assignee_ids else '',
              'label_ids': f'label_ids: {str_label_ids}' if label_ids else '',
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updateCard', {}).get('card')
        
        except Exception as err:
          raise exceptions(err)
      
      
    def create_card_phase(self, pipe_id, fields_attributes, due_date="", parent_ids=[], phase_id=None, response_fields=None, headers={}, label_ids=[]):
        """ Create card: Mutation to create a card, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'card { id title }'
          query = '''
              mutation {
                createCard(
                  input: {
                    label_ids: %(label_ids)s
                    due_date: %(due_date)s
                    pipe_id: %(pipe_id)s
                    phase_id: %(phase_id)s
                    fields_attributes: %(fields_attributes)s
                    parent_ids: [ %(parent_ids)s ]
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'label_ids': json.dumps(label_ids),
              'pipe_id': json.dumps(pipe_id),
              'phase_id': json.dumps(phase_id),
              'due_date': json.dumps(due_date),
              'fields_attributes': self.__prepare_json_dict(fields_attributes),
              'parent_ids': ', '.join([json.dumps(id) for id in parent_ids]),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createCard', {}).get('card')
        
        except Exception as err:
          raise exceptions(err)
      
      
    def update_fields_card(self, node_id, field_id=None, new_value=None, values:dict=None, response_fields=None, headers={}):
      """ Update fields card: Mutation to update fields a card, in case of success a query is returned. """
      try:
        values = values or { "fieldId": f"{field_id}", "value": f'{new_value}' }
        response_fields = response_fields or "clientMutationId success"
        
        query = '''
            mutation{
              updateFieldsValues(input:{
                nodeId:%(node_id)s
                values:%(values)s
              }) {
                %(response_fields)s
              }
            }
        '''%{
          'node_id':json.dumps(node_id),
          'response_fields': response_fields,
          'values': self.__prepare_json_dict(values)
        }
        return self.request(query, headers).get('data', {}).get('updateFieldsValues', {}).get('success')
      
      except Exception as err:
          raise exceptions(err)
    
    
    def delete_card(self, id, response_fields=None, headers={}):
        """ Delete card: Mutation to delete a card, in case of success success: true is returned. """
        try:
          
          response_fields = response_fields or 'success'
          query = 'mutation { deleteCard(input: { id: %(id)s }) { %(response_fields)s } }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deleteCard', {})
        
        except Exception as err:
          raise exceptions(err)


    def move_card_to_phase(self, card_id, destination_phase_id, response_fields=None, headers={}):
        """ Move card to phase: Mutation to move a card to a phase, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'card{ id current_phase { name } }'
          query = '''
              mutation {
                moveCardToPhase(
                  input: {
                    card_id: %(card_id)s
                    destination_phase_id: %(destination_phase_id)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'card_id': json.dumps(card_id),
              'destination_phase_id': json.dumps(destination_phase_id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('moveCardToPhase', {}).get('card')
        
        except Exception as err:
          raise exceptions(err)


    def update_card_field(self, card_id, field_id, new_value, response_fields=None, headers={}):
        """ Update card field: Mutation to update a card field, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'card{ id }'
          query = '''
              mutation {
                updateCardField(
                  input: {
                    card_id: %(card_id)s
                    field_id: %(field_id)s
                    new_value: %(new_value)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'card_id': json.dumps(card_id),
              'field_id': json.dumps(field_id),
              'new_value': json.dumps(new_value),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updateCardField', {}).get('card')
        
        except Exception as err:
          raise exceptions(err)


    def create_comment(self, card_id, text, response_fields=None, headers={}):
        """ Create comment: Mutation to create a comment, in case of success a query is returned. """
        try:
          
          response_fields = response_fields or 'comment { id text }'
          query = '''
              mutation {
                createComment(
                  input: {
                    card_id: %(card_id)s
                    text: %(text)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'card_id': json.dumps(card_id),
              'text': json.dumps(text),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createComment', {}).get('comment')
        
        except Exception as err:
          raise exceptions(err)


    def update_comment(self, id, text, response_fields=None, headers={}):
        """ Update comment: Mutation to update a comment, in case of success a query is returned. """
        try:
          
          response_fields = response_fields or 'comment { id text }'
          query = '''
              mutation {
                updateComment(
                  input: {
                    id: %(id)s
                    text: %(text)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'id': json.dumps(id),
              'text': json.dumps(text),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updateComment', {}).get('comment')
        
        except Exception as err:
          raise exceptions(err)

    def delete_comment(self, id, response_fields=None, headers={}):
        """ Delete comment: Mutation to delete a comment, in case of success success: true is returned. """
        try:
          
          response_fields = response_fields or 'success'
          query = 'mutation { deleteComment(input: { id: %(id)s }) { %(response_fields)s }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deleteComment', {})
        
        except Exception as err:
          raise exceptions(err)


    def set_role(self, pipe_id, member, response_fields=None, headers={}):
        """ Set role: Mutation to set a user's role, in case of success a query is returned. """
        try:
          
          response_fields = response_fields or 'member{ user{ id } role_name }'
          query = '''
              mutation {
                setRole(
                  input: {
                    pipe_id: %(pipe_id)s
                    member: %(member)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'pipe_id': json.dumps(pipe_id),
              'member': self.__prepare_json_dict(member),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('setRole', {}).get('comment')
        
        except Exception as err:
          raise exceptions(err)


    def pipe_relations(self, ids, response_fields=None, headers={}):
        """ Show pipe relations: Get pipe relations by their identifiers. """
        try:
          
          response_fields = response_fields or 'id name allChildrenMustBeDoneToMoveParent allChildrenMustBeDoneToFinishParent' \
                                  ' canCreateNewItems canConnectExistingItems canConnectMultipleItems childMustExistToMoveParent ' \
                                  'childMustExistToFinishParent'
          query = '{ pipe_relations(ids: [%(ids)s]) { %(response_fields)s } }' % {
              'ids': ', '.join([json.dumps(id) for id in ids]),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('pipe_relations')
        
        except Exception as err:
          raise exceptions(err)


    def create_pipe_relation(self, parent_id, child_id, name, all_children_must_be_done_to_finish_parent, child_must_exist_to_move_parent,
            child_must_exist_to_finish_parent, all_children_must_be_done_to_move_parent, can_create_new_items, can_connect_existing_items,
            can_connect_multiple_items, response_fields=None, headers={}):
        """ Create pipe relation: Mutation to create a pipe relation between two pipes, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'pipeRelation { id name }'
          query = '''
              mutation {
                createPipeRelation(
                  input: {
                    parentId: %(parent_id)s
                    childId: %(child_id)s
                    name: %(name)s
                    allChildrenMustBeDoneToFinishParent: %(all_children_must_be_done_to_finish_parent)s
                    childMustExistToMoveParent: %(child_must_exist_to_move_parent)s
                    childMustExistToFinishParent: %(child_must_exist_to_finish_parent)s
                    allChildrenMustBeDoneToMoveParent: %(all_children_must_be_done_to_move_parent)s
                    canCreateNewItems: %(can_create_new_items)s
                    canConnectExistingItems: %(can_connect_existing_items)s
                    canConnectMultipleItems: %(can_connect_multiple_items)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'parent_id': json.dumps(parent_id),
              'child_id': json.dumps(child_id),
              'name': json.dumps(name),
              'all_children_must_be_done_to_finish_parent': json.dumps(all_children_must_be_done_to_finish_parent),
              'child_must_exist_to_move_parent': json.dumps(child_must_exist_to_move_parent),
              'child_must_exist_to_finish_parent': json.dumps(child_must_exist_to_finish_parent),
              'all_children_must_be_done_to_move_parent': json.dumps(all_children_must_be_done_to_move_parent),
              'can_create_new_items': json.dumps(can_create_new_items),
              'can_connect_existing_items': json.dumps(can_connect_existing_items),
              'can_connect_multiple_items': json.dumps(can_connect_multiple_items),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createPipeRelation', {}).get('pipeRelation')
        
        except Exception as err:
          raise exceptions(err)


    def update_pipe_relation(self, id, name, all_children_must_be_done_to_finish_parent, child_must_exist_to_move_parent,
            child_must_exist_to_finish_parent, all_children_must_be_done_to_move_parent, can_create_new_items, can_connect_existing_items,
            can_connect_multiple_items, response_fields=None, headers={}):
        """ Update pipe relation: Mutation to update a pipe relation, in case of success a query is returned. """
        try:
          
          response_fields = response_fields or 'pipeRelation { id name }'
          query = '''
              mutation {
                updatePipeRelation(
                  input: {
                    id: %(id)s
                    name: %(name)s
                    allChildrenMustBeDoneToFinishParent: %(all_children_must_be_done_to_finish_parent)s
                    childMustExistToMoveParent: %(child_must_exist_to_move_parent)s
                    childMustExistToFinishParent: %(child_must_exist_to_finish_parent)s
                    allChildrenMustBeDoneToMoveParent: %(all_children_must_be_done_to_move_parent)s
                    canCreateNewItems: %(can_create_new_items)s
                    canConnectExistingItems: %(can_connect_existing_items)s
                    canConnectMultipleItems: %(can_connect_multiple_items)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'id': json.dumps(id),
              'name': json.dumps(name),
              'all_children_must_be_done_to_finish_parent': json.dumps(all_children_must_be_done_to_finish_parent),
              'child_must_exist_to_move_parent': json.dumps(child_must_exist_to_move_parent),
              'child_must_exist_to_finish_parent': json.dumps(child_must_exist_to_finish_parent),
              'all_children_must_be_done_to_move_parent': json.dumps(all_children_must_be_done_to_move_parent),
              'can_create_new_items': json.dumps(can_create_new_items),
              'can_connect_existing_items': json.dumps(can_connect_existing_items),
              'can_connect_multiple_items': json.dumps(can_connect_multiple_items),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updatePipeRelation', {}).get('pipeRelation')
        
        except Exception as err:
          raise exceptions(err)


    def delete_pipe_relation(self, id, response_fields=None, headers={}):
        """ Delete pipe relation: Mutation to delete a pipe relation, in case of success success: true is returned. """
        try:
          response_fields = response_fields or 'success'
          query = 'mutation { deletePipeRelation(input: { id: %(id)s }) { %(response_fields)s }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deletePipeRelation', {})
        
        except Exception as err:
          raise exceptions(err)


    def tables(self, ids, response_fields=None, headers={}):
        """ List tables: Get tables through table ids. """
        try:
          response_fields = response_fields or 'id name url'
          query = '{ tables(ids: [%(ids)s]) { %(response_fields)s } }' % {
              'ids': ', '.join([json.dumps(id) for id in ids]),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('tables')
        
        except Exception as err:
          raise exceptions(err)


    def table(self, id, response_fields=None, headers={}):
        """ Show table: Get a table through table id. """
        try:
          
          response_fields = response_fields or 'authorization create_record_button_label description' \
              ' icon id labels { id } members { role_name user { id } } my_permissions { can_manage_record ' \
              'can_manage_table } name public public_form summary_attributes { id } summary_options { name } ' \
              'table_fields { id } table_records { edges { node { id } } } table_records_count title_field { id } url }'
          query = '{ table(id: %(id)s) { %(response_fields)s } }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('table')
        
        except Exception as err:
          raise exceptions(err)


    def create_table(self, organization_id, name, description, public, authorization, response_fields=None, headers={}):
        """ Create table: Mutation to create a table, in case of success a query is returned. """
        try:
          
          response_fields = response_fields or 'table { id name description public authorization }'
          query = '''
              mutation {
                createTable(
                  input: {
                    organization_id: %(organization_id)s
                    name: %(name)s
                    description: %(description)s
                    public: %(public)s
                    authorization: %(authorization)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'organization_id': json.dumps(organization_id),
              'name': json.dumps(name),
              'description': json.dumps(description),
              'public': json.dumps(public),
              'authorization': json.dumps(authorization),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createTable', {}).get('table')
        
        except Exception as err:
          raise exceptions(err)


    def update_table(self, id, name, description, public, authorization, icon, create_record_button_label,
            title_field_id, public_form, summary_attributes,  response_fields=None, headers={}):
        """ Update table: Mutation to update a table, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'table { id name description public authorization }'
          query = '''
              mutation {
                updateTable(
                  input: {
                    id: %(id)s
                    name: %(name)s
                    description: %(description)s
                    public: %(public)s
                    authorization: %(authorization)s
                    icon: %(icon)s
                    create_record_button_label: %(create_record_button_label)s
                    title_field_id: %(title_field_id)s
                    public_form: %(public_form)s
                    summary_attributes: [ %(summary_attributes)s ]
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'id': json.dumps(id),
              'name': json.dumps(name),
              'description': json.dumps(description),
              'public': json.dumps(public),
              'authorization': json.dumps(authorization),
              'icon': json.dumps(icon),
              'create_record_button_label': json.dumps(create_record_button_label),
              'title_field_id': json.dumps(title_field_id),
              'public_form': json.dumps(public_form),
              'summary_attributes': ', '.join([json.dumps(summary) for summary in summary_attributes]),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updateTable', {}).get('table')
        
        except Exception as err:
          raise exceptions(err)


    def delete_table(self, id, response_fields=None, headers={}):
        """ Delete table: Mutation to delete a table, in case of success a query with the field success is returned. """
        try:
          response_fields = response_fields or 'success'
          query = 'mutation { deleteTable(input: { id: %(id)s }) { %(response_fields)s }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deleteTable', {})
        
        except Exception as err:
          raise exceptions(err)

    def create_table_field(self, table_id, type, label, options, description, help, required,
            minimal_view, custom_validation, response_fields=None, headers={}):
        """ Create table field: Mutation to create a table field, in case of success a query is returned. """
        
        try:
          response_fields = response_fields or 'table_field { id label type options description help required minimal_view custom_validation }'
          query = '''
              mutation {
                createTableField(
                  input: {
                    table_id: %(table_id)s
                    type: %(type)s
                    label: %(label)s
                    options: %(options)s
                    description: %(description)s
                    help: %(help)s
                    required: %(required)s
                    minimal_view: %(minimal_view)s
                    custom_validation: %(custom_validation)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'table_id': json.dumps(table_id),
              'type': json.dumps(type),
              'label': json.dumps(label),
              'options': self.__prepare_json_list(options),
              'description': json.dumps(description),
              'help': json.dumps(help),
              'required': json.dumps(required),
              'minimal_view': json.dumps(minimal_view),
              'custom_validation': json.dumps(custom_validation),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createTableField', {}).get('table_field')
        
        except Exception as err:
          raise exceptions(err)
        

    def update_table_field(self, table_id, id, label, options, description, help, required,
            minimal_view, custom_validation, response_fields=None, headers={}):
        """ Update table field: Mutation to update a table field, in case of success a query is returned. """
        
        try:
          response_fields = response_fields or 'table_field { id label type options description help required minimal_view custom_validation }'
          query = '''
              mutation {
                updateTableField(
                  input: {
                    table_id: %(table_id)s
                    id: %(id)s
                    label: %(label)s
                    options: %(options)s
                    description: %(description)s
                    help: %(help)s
                    required: %(required)s
                    minimal_view: %(minimal_view)s
                    custom_validation: %(custom_validation)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'table_id': json.dumps(table_id),
              'id': json.dumps(id),
              'label': json.dumps(label),
              'options': self.__prepare_json_list(options),
              'description': json.dumps(description),
              'help': json.dumps(help),
              'required': json.dumps(required),
              'minimal_view': json.dumps(minimal_view),
              'custom_validation': json.dumps(custom_validation),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updateTableField', {}).get('table_field')

        except Exception as err:
          raise exceptions(err)


    def set_table_field_order(self, table_id, field_ids, response_fields=None, headers={}):
        """ Set table record field value Mutation to set a table field order, in case of success a query with the field success is returned. """
        try:
          response_fields = response_fields or 'table_field { id }'
          query = '''
              mutation {
                setTableFieldOrder(
                  input: {
                    table_id: %(table_id)s
                    field_ids: %(field_ids)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'table_id': json.dumps(table_id),
              'field_ids': self.__prepare_json_list(field_ids),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('setTableFieldOrder', {}).get('table_field')
        
        except Exception as err:
          raise exceptions(err)


    def delete_table_field(self, table_id, id, response_fields=None, headers={}):
        """ Delete table field: Mutation to delete a table field, in case of success a query with the field success is returned. """
        try:
          response_fields = response_fields or 'success'
          query = 'mutation { deleteTableField(input: { table_id: %(table_id)s id: %(id)s }) { %(response_fields)s }' % {
              'table_id': json.dumps(table_id),
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deleteTableField', {})

        except Exception as err:
          raise exceptions(err)

    def table_records(self, table_id, first=10, response_fields=None, headers={}, search={}):
        """ List table records: Get table records with pagination through table id. """
        try:
          response_fields = response_fields or 'edges { cursor node { id title url } } pageInfo { endCursor hasNextPage hasPreviousPage startCursor }'
          query = '{ table_records(first: %(first)s, table_id: %(table_id)s, search: %(search)s) { %(response_fields)s } }' % {
              'first': json.dumps(first),
              'table_id': json.dumps(table_id),
              'response_fields': response_fields,
              'search': self.__prepare_json_dict(search),
          }
          return self.request(query, headers).get('data', {}).get('table_records')
        
        except Exception as err:
          raise exceptions(err)


    def table_record(self, id, response_fields=None, headers={}):
        """ Show table record: Get table record through table record id. """
        try:
          response_fields = response_fields or 'assignees { id name } created_at created_by { id name } due_date' \
              ' finished_at id labels { id name } parent_relations { name source_type } record_fields { array_value ' \
              'field {id} date_value datetime_value filled_at float_value name required updated_at value } summary { title value } ' \
              'table { id } title updated_at url }'
          query = '{ table_record(id: %(id)s) { %(response_fields)s } ' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('table_record')
        
        except Exception as err:
          raise exceptions(err)


    def create_table_record(self, table_id, title='', due_date=None, fields_attributes=[], response_fields=None, headers={}):
        """ Create table record: Mutation to create a table record, in case of success a query is returned. """
        try:
          response_fields = response_fields or 'table_record { id title due_date record_fields { name value } }'
          query = '''
              mutation {
                createTableRecord(
                  input: {
                    table_id: %(table_id)s
                    %(title)s
                    %(due_date)s
                    fields_attributes: %(fields_attributes)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'table_id': json.dumps(table_id),
              'title': u'title: %s' % json.dumps(title) if title else '',
              'due_date': u'due_date: %s' % due_date.strftime('%Y-%m-%dT%H:%M:%S+00:00') if due_date else '',
              'fields_attributes': self.__prepare_json_list(fields_attributes),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('createTableRecord', {}).get('table_record')
        
        except Exception as err:
          raise exceptions(err)


    def update_table_record(self, id, title, due_date, response_fields=None, headers={}):
        """ Update table record: Mutation to update a table record, in case of success a query is returned. """
        try:
          
          response_fields = response_fields or 'table_record { id title due_date record_fields { name value } }'
          query = '''
              mutation {
                updateTableRecord(
                  input: {
                    id: %(id)s
                    title: %(title)s
                    due_date: %(due_date)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'id': json.dumps(id),
              'title': json.dumps(title),
              'due_date': due_date.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('updateTableRecord', {}).get('table_record')
        
        except Exception as err:
          raise exceptions(err)


    def set_table_record_field_value(self, table_record_id, field_id, value, response_fields=None, headers={}):
        """ Set table record field value: Mutation to set a table record field value, in case of success a query with the field success is returned. """
        try:
          response_fields = response_fields or 'table_record { id title } table_record_field { value }'
          query = '''
              mutation {
                setTableRecordFieldValue(
                  input: {
                    table_record_id: %(table_record_id)s
                    field_id: %(field_id)s
                    value: %(value)s
                  }
                ) { %(response_fields)s }
              }
          ''' % {
              'table_record_id': json.dumps(table_record_id),
              'field_id': json.dumps(field_id),
              'value': json.dumps(value),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('setTableRecordFieldValue', {})
        
        except Exception as err:
          raise exceptions(err)


    def delete_table_record(self, id, response_fields=None, headers={}):
        """ Delete table record: Mutation to delete a table record, in case of success a query with the field success is returned. """
        try:
          response_fields = response_fields or 'success'
          query = 'mutation { deleteTableRecord(input: { id: %(id)s }) { %(response_fields)s }}' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {}).get('deleteTableRecord', {})
        
        except Exception as err:
          raise exceptions(err)


    def fields_pipe(self, id, response_fields=None, headers={}):
        """ Show fields: Get a pipe by its identifier. """
        try:
          response_fields = response_fields or 'cards_count phases { name fields { id label } }' \
                                              ' start_form_fields { id label }'
          query = '{ pipe (id: %(id)s) { %(response_fields)s } }' % {
              'id': json.dumps(id),
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {})
        
        except Exception as err:
          raise exceptions(err)


    def check_webhook(self, id:list, response_fields=None, headers={}):
        """ Show Webhooks: Get a pipe by its identifier. """
        try:
          if type(id) != list:
            raise exceptions(f"Check type the id!")
          
          ids = int(id[0]) if len(id) == 1 else [int(i) for i in id]
          response_fields = response_fields or 'webhooks{ id name actions url email headers }'
          
          query = '{ pipes (ids: %(id)s) { %(response_fields)s } }' % {
              'id': ids,
              'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {})
        
        except Exception as err:
          raise exceptions(err)

    def create_webhook(self, pipe_id:int, email:str, actions:list, name_webhook:str, url:str, response_fields=None, headers={}):
        """ Show Webhooks: Get a pipe by its identifier. """
        try:
          response_fields = response_fields or 'webhooks{ id actions url}'
          
          query = '''
              mutation {
              createWebhook(
                  input: {
                      email: %(email)s,
                      actions: %(actions)s,
                      name: %(name_webhook)s,
                      pipe_id: %(pipe_id)s,
                      url: %(url)s
                  }
              ) { %(response_fields)s }
              }
          ''' % {
                  'email':email,
                  'actions':actions,
                  'name_webhook':name_webhook,
                  'pipe_id': pipe_id,
                  'url':url,
                  'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {})
        
        except Exception as err:
          raise exceptions(err)


    def create_presigned_url(self, organization_id:int, file_name_path:str, response_fields=None, client_mutation_id:str=None, content_type:str=None, headers={}):
        """ Created Url: Get a Organization ID and FilePath to created url. """
        try:
          response_fields = response_fields or 'url downloadUrl'
          
          query = '''
              mutation {
              createPresignedUrl(
                  input: {
                      organizationId: %(organization_id)s,
                      fileName: "%(file_name_path)s",
                      %(client_mutation_id)s,
                      %(content_type)s,
                  }
              ) { %(response_fields)s }
              }
          ''' % {
                  'organization_id':organization_id,
                  'file_name_path':file_name_path,
                  'client_mutation_id':f'clientMutationId:{client_mutation_id}' if client_mutation_id else '',
                  'content_type': f'contentType:{content_type}' if content_type else '',
                  'response_fields': response_fields,
          }
          return self.request(query, headers).get('data', {})
        
        except Exception as err:
          raise exceptions(err)