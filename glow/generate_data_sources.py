from connectors.tableau.tableau import TableauConnector
from posixpath import join
from typing import List, Dict, Tuple

import argparse
import connectors.tableau
import os 
import utils
import logging
import sys
import yaml

logging.basicConfig(level=logging.INFO)

MAIN_PATH = '/Users/tomevers/projects/airglow'
CONNECTIONS_CONF_FILE = 'airglow_connections.yml'

DS_FILENAME = 'data sources.yml'
DS_TEMPLATE = 'templates/data_source.md'


class ConnectionValidationError(Exception):
    pass

def get_connections_config(yaml_format=True) -> dict:
    yaml_file = os.path.join(MAIN_PATH, CONNECTIONS_CONF_FILE)
    try:
        return utils.get_file(yaml_file, yaml_format)
    except FileNotFoundError:
        logging.exception(FileNotFoundError('Airglow connections file can not be found.'))
        sys.exit(1)


def store_ds(events_md: str, event: dict, docs_dir: str):
    file_dir = os.path.join(docs_dir, 'data sources', event['category'])
    file_name = event['name'] + '.md'
    if not os.path.isdir(file_dir):
        os.makedirs(file_dir)
    with open(os.path.join(file_dir, file_name), 'w') as file:
        file.write(events_md)


def generate_datasources_yaml():
    conn_config = get_connections_config()
    if 'connections' not in conn_config.keys():
        logging.exception('connections info not found in airglow_connections config file.')
        sys.exit(1)
    tableau_config = conn_config['connections']['tableau']
    tableau_connector = TableauConnector(server=tableau_config['server'], 
                                        sitename=tableau_config['sitename'], 
                                        password=tableau_config['password'],
                                        username=tableau_config['username'])
    ds = tableau_connector.fetch_datasources()
    ds = [tableau_connector.generate_datasource_dag(datasource) for datasource in ds]  

    logging.info("storing data source")
    
    with open(r'/Users/tomevers/projects/airglow/definitions/data sources.yml', 'w') as file:
        documents = yaml.dump(ds, file, sort_keys=False)

    return ds


def generate_markdown(datasource):
    template_path = os.path.join(MAIN_PATH, DS_TEMPLATE)
    with open(template_path, 'r') as file:
        ds_md = file.read()
    ds_md = ds_md.replace('{<yaml_header>}', yaml.dump(datasource))
    
    return ds_md


def get_datasource_definitions(yaml_format=True) -> dict:
    """ returns the data source definition yaml file as a dict.
    Returns:
        a dict with all data sources defined in the yaml file.
    """
    yaml_file = os.path.join(MAIN_PATH, 'definitions', DS_FILENAME)
    try:
        return utils.get_file(yaml_file, yaml_format)
    except FileNotFoundError:
        logging.exception(FileNotFoundError('Datasource definition file can not be found.'))
        sys.exit(1)


def main(args):
    logging.info('Starting datasource generation script..')
    logging.info('****************************************')
    logging.info('** Step 1: Get all information')
    logging.info('****************************************')
    if args.use_local_definitions.lower() in ('true', '1', 't'):
        logging.info('** Retrieving data source definitions from local yaml file')
        datasource_defs = get_datasource_definitions()
    else:
        logging.info('** Retrieving data source definitions from Tableau')
        datasource_defs = generate_datasources_yaml()
    
    logging.info('****************************************')
    logging.info('** Step 2: Generate and store event files.')
    logging.info('****************************************')
    for datasource in datasource_defs:
        logging.info('generating datasource md file for {}'.format(datasource['data_source_name']))
        ds_md = generate_markdown(datasource)
        utils.store_md(ds_md, 'data sources', datasource['data_source_project'], datasource['data_source_name'],  args.docs_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser('Script to convert event definitions file into markdown format.')
    parser.add_argument('--docs_dir', type=str,
                        help='path to the folder where the generated docs should be stored. The script will need write access to this folder. Defaults to "./docs/"')
    parser.add_argument('--use_local_definitions', type=str,
                        help='path to the folder where the generated docs should be stored. The script will need write access to this folder. Defaults to "./docs/"')
    args = parser.parse_args()
    main(args)
