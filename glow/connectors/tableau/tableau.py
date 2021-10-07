import tableauserverclient as TSC
import os
import logging
import zipfile
import xml.etree.ElementTree as ET
import shutil
import connectors.tableau.tableau_client as tc
import re 
from tableauserverclient.server.endpoint import datasources_endpoint

TABLEAU_VERSION = '3.13'
TEMP_PATH = '/Users/tomevers/projects/airglow/temp'
TABLEAU_PATH = 'tableau'

class TableauConnector():
    server = None
    sitename = None
    username = None
    password = None
    tableau_server = None
    
    all_datasources = None
    all_tasks = None
    all_schedules = None

    def __init__(self, server, sitename, username, password) -> None:
        self.server = server
        self.sitename = sitename
        self.username = username
        self.password = password
        self.tableau_auth = TSC.TableauAuth(self.username, self.password, self.sitename)
        self.tableau_server = TSC.Server(self.server)
        self.tableau_server.version = TABLEAU_VERSION
        self.tableau_client = tc.TableauClient(server=server, sitename=sitename, username=username, password=password)

    def _get_tasks(self):
        if self.all_tasks is None:
            self.all_tasks = self.tableau_client.get_tasks()
        return self.all_tasks 


    def _get_schedule_for_datasource(self, datasource_id):
        all_tasks = self._get_tasks()

        schedules = []
        for task in all_tasks:
            if task['type'] == 'RefreshExtractTask' and task['target_type'] == 'datasource' and task['target_id'] == datasource_id:
                schedules.append({
                    'id': task['schedule_id'],
                    'frequency': task['frequency'],
                    'state': task['state'],
                    'next_run_at': task['next_run_at']
                })
        return schedules


    def _get_relationships_xml(self, datasource_id, datasource_name):
        tableau_folder = os.path.join(TEMP_PATH, TABLEAU_PATH)
        if not os.path.isdir(tableau_folder):
            os.makedirs(tableau_folder)
        
        # download data source
        file_path = os.path.join(tableau_folder, datasource_name)
        ds_path = self.tableau_server.datasources.download(datasource_id, filepath=file_path, include_extract=False)

        # unzip data source
        ds_folder = os.path.join(tableau_folder, datasource_name)
        if not os.path.isdir(ds_folder):
            os.makedirs(ds_folder)
        with zipfile.ZipFile(ds_path, 'r') as zip_ref:
            zip_ref.extractall(ds_folder)

        # load data source
        
        for filename in os.listdir(ds_folder):
            if filename.endswith(".tds"): 
                tree = ET.parse(os.path.join(ds_folder, filename))
        
        if tree is None:
            logging.warning('could not process Tableau data source for data source with name {}'.format(datasource_name))
        
        # cleanup folder and files
        shutil.rmtree(ds_folder)
        os.remove(ds_path)

        return tree

    def fetch_datasources(self):
        owners = {}

        with self.tableau_server.auth.sign_in(self.tableau_auth):
            all_datasources, pagination_item = self.tableau_server.datasources.get()
            logging.info("{} datasources found.".format(pagination_item.total_available))

            datasources = []
            for datasource in all_datasources:
                print(datasource.name)
                clean_datasource = {}
                clean_datasource['data_source_name'] = datasource.name
                clean_datasource['data_source_type'] = 'Tableau Data Source'
                clean_datasource['data_source_project'] = datasource.project_name
                clean_datasource['data_source_url'] = datasource.webpage_url
                clean_datasource['data_source_description'] = datasource.description
                clean_datasource['data_source_created_at'] = datasource.created_at
                clean_datasource['data_source_updated_at'] = datasource.updated_at

                if datasource.owner_id not in owners.keys():
                    user = self.tableau_server.users.get_by_id(datasource.owner_id)
                    owners[datasource.owner_id] = user.name
                
                clean_datasource['data_source_owner'] = {
                    'name': owners[datasource.owner_id]
                }
                clean_datasource['data_source_materialisation'] = {}
                if datasource.has_extracts:
                    clean_datasource['data_source_materialisation']['type'] = 'Extract'
                else:
                    clean_datasource['data_source_materialisation']['type'] = 'Live'

                clean_datasource['data_source_materialisation']['schedules'] = self._get_schedule_for_datasource(datasource.id)

                self.tableau_server.datasources.populate_connections(datasource)
                if len(datasource.connections) >= 1:
                    clean_datasource['data_source_materialisation']['db_username'] = datasource.connections[0].username
                else:
                    clean_datasource['data_source_materialisation']['db_username'] = ''

                clean_datasource['raw_relationships_xml'] = self._get_relationships_xml(datasource.id, datasource.name)
                
                if clean_datasource['raw_relationships_xml'].findall('date-options'):
                    for date_option in clean_datasource['raw_relationships_xml'].findall('date-options'):
                        if 'start-of-week' in date_option.attrib.keys():
                            clean_datasource['data_source_materialisation']['week_start'] = date_option.attrib['start-of-week']
                            break
                datasources.append(clean_datasource)

        return datasources
    
    def _get_relation_query(self, expression):
        if expression.attrib['op'].lower() == '=':
            relation_names_1, sql_snippet_1 = self._get_relation_query(expression[0])
            relation_names_2, sql_snippet_2 = self._get_relation_query(expression[1])
            sql = sql_snippet_1 + ' = ' + sql_snippet_2
            return relation_names_1 + relation_names_2, sql
        elif expression.attrib['op'].lower() in ('and', 'or'):
            relation_names_1, sql_snippet_1 = self._get_relation_query(expression[0])
            relation_names_2, sql_snippet_2 = self._get_relation_query(expression[1])
            sql = '({sql_1}) {op} ({sql_2})'.format(sql_1=sql_snippet_1, op=expression.attrib['op'].lower(), sql_2=sql_snippet_2)
            
            for rel in relation_names_2:
                if rel not in relation_names_1:
                    logging.warning('JOIN between mutliple tables is currently not support. the assumed relations might not appear correct.')
            return relation_names_1, sql
        else:
            relation_name = re.findall('\[[^\]]*\]', expression.attrib['op'])[0][1:-1]
            sql = re.sub('\[[^\]]*\]', '{TABLE}', expression.attrib['op'], 1)
            return [relation_name], sql


    def _convert_xml_relation(self, relations):
        """
        returns a list of relations 
        """

        if relations.attrib['type'] == 'join':
            
            rels = self._convert_xml_relation(relations[1])
            rels.update(self._convert_xml_relation(relations[2]))
            
            expressions = relations.find('clause').find('expression')
            relation_names, sql = self._get_relation_query(expressions)

            to = relation_names[0]
            name  = relation_names[1]

            rels[name]['relation_type'] = relations.attrib['join'] + '_join'
            rels[name]['to'] = to
            
            rels[name]['sql'] = sql

            return rels
        
        elif relations.attrib['type'] == 'union':
            # assumption that all child in a union are tables.
            logging.warning('union found in relations xml. Unions are currently only support for children with type table.')
            rels = {}
            name = relations.attrib['name']
            query = ''
            if relations.attrib['all'] == 'true':
                union = 'union all'
            else:
                union = 'union'
             
            first = True
            for child in relations:
                if not first:
                    query += '\n' + union + '\n'
                first = False
                query += 'select * from ' + \
                child.attrib['table']

            return {name: {
                'type': 'query',
                'name': name,
                'query': query
            }}


        elif relations.attrib['type'] == 'table':
            return {relations.attrib['name']: {
                    'type': 'model',
                    'model': relations.attrib['table'],
                    'name':  relations.attrib['name']}}

        elif relations.attrib['type'] == 'text':
            return {relations.attrib['name']: {
                    'type': 'query',
                    'name': relations.attrib['name'],
                    'query':  relations.text}}

        else:
            logging.warning('XML relation type not recognised. ')
            return {}

    def generate_datasource_dag(self, datasource):
        if 'raw_relationships_xml' not in datasource.keys():
            logging.warning('raw_relationships_xml key not available for datasource {}'.format(datasource['name']))
            return datasource
        
        connection = datasource['raw_relationships_xml'].findall('connection')[0]
        if connection is None:
            logging.warning('no connection found in xml for datasource {}'.format(datasource['name']))
            return datasource
        
        relations = connection.find('relation')
        if relations is None:
            relations = connection.find('_.fcp.ObjectModelEncapsulateLegacy.false...relation') 
            if relations is None:
                logging.warning('no relationships found in xml for datasource {}'.format(datasource['name']))
                return datasource

        rels = self._convert_xml_relation(relations)
        clean_relations = []
        for _, rel in rels.items():
            if 'relation_type' not in rel.keys():
                rel['relation_type'] = 'from'
            clean_relations.append(rel)

        datasource['relations'] = clean_relations
        del datasource['raw_relationships_xml']
        return datasource
