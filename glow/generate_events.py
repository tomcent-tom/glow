from posixpath import join
from typing import List, Dict, Tuple
import logging
import argparse
import os
import shutil
import json
import yaml
import sys 
import git
import subprocess
import SnowflakeQuery as sql
import pandas as pd
import UsageChartGenerator
import utils

logging.basicConfig(level=logging.INFO)

MAIN_PATH = '/Users/tomevers/projects/airglow'
AMP_BASE_URL = 'https://amplitude.com/api/2/taxonomy/'
ED_FILENAME = 'event_definitions.yml'
MD_FILENAME = 'model_definitions.yml'
EVENT_TEMPLATE = 'templates/event.md'
EVENT_DEFINITIONS_GIT = 'https://github.com/airtasker/airtasker_event_definitions.git'
EVENT_DEFINITIONS_GIT_FOLDER = 'event_definitions_git_clone'

ENABLE_SQL_QUERIES = os.getenv("ENABLE_SQL_QUERIES", 'True').lower() in ('true', '1', 't')


def get_event_definitions(yaml_format=True) -> dict:
    """ returns the event definition yaml file as a dict.
    Returns:
        a dict with all event types defined in the yaml file.
    """
    yaml_file = os.path.join(EVENT_DEFINITIONS_GIT_FOLDER, ED_FILENAME)
    try:
        return utils.get_file(yaml_file, yaml_format)
    except FileNotFoundError:
        logging.exception(FileNotFoundError('Event definition file can not be found.'))
        sys.exit(1)


def get_model_definitions(yaml_format=True) -> dict:
    """ returns the model definition yaml file as a dict.
    Returns:
        a dict with all event types defined in the yaml file.
    """
    yaml_file = os.path.join(EVENT_DEFINITIONS_GIT_FOLDER, MD_FILENAME)
    try:
        return utils.get_file(yaml_file, yaml_format)
    except FileNotFoundError:
        logging.exception(FileNotFoundError('Model definition file can not be found.'))
        sys.exit(1)


def clean_model_definitions(models: dict) -> dict:
    cleaned_models = {}
    for model_name, model in models.items():
        cleaned_model = []
        for prop_name, prop_values in model.items():
            prop = {
                'parameter_name': prop_name,
                'type': prop_values['type'] if 'type' in prop_values.keys() else '',
                'description': prop_values['description'] if 'description' in prop_values.keys() else '',
                'allowed': prop_values['allowed'] if 'allowed' in prop_values.keys() else []
            }
            cleaned_model.append(prop)
        cleaned_models[model_name] = cleaned_model
    return cleaned_models

def _get_model_properties(event: dict, model_defs: dict) -> dict:
    if 'models' not in event.keys():
        return {}
    
    return {model_name: model for (model_name, model) in model_defs.items() if model_name in event['models'] }
    

def _get_platforms(event: dict) -> str:
    if 'platforms' not in event.keys():
        return ['web', 'Android', 'iOS']
    else: 
        return event['platforms']


def _get_created_info(event_key: str, repo: git.Reference.repo) -> str:
    ps = subprocess.Popen(("git",
                                "--git-dir",
                                os.path.join(EVENT_DEFINITIONS_GIT_FOLDER ,".git"),
                                "log",
                                '--reverse',
                                '--pretty=%cs|%an|%H',
                                '-S'+event_key,
                                ED_FILENAME), stdout=subprocess.PIPE)
    output = subprocess.check_output(('head', '-n', '1'), stdin=ps.stdout)
    ps.wait()
    creation_date, author, hash = output.decode('UTF-8')[:-1].split('|') # remove '\n' 
    return creation_date, author, hash


def _get_lines_of_event(event_key: str, event_file:List) -> Tuple[int, int]:
    start, length = 0, 0
    
    for line, value in enumerate(event_file):
        if event_key in value:
            start = line+1
            break
    
    for line, value in enumerate(event_file[start:], start):
        if len(value) == 1:
            length = line - start
            break
    return start, length


def _get_last_modified_info(event_key: str, event_file: List) -> str:
    start, length = _get_lines_of_event(event_key, event_file)

    if start == 0 or length == 0 or start+length >= len(event_file):
        logging.warning("Invalid log range for keyword {event_key}. start: {start}, length: {length}, length of file: {file_length}".format(event_key=event_key, start=start, length=length, file_length=len(event_file)))
        return '', '', '' # TODO: fix last line on event file.

    ps = subprocess.Popen(("git",
                                "--git-dir",
                                os.path.join(EVENT_DEFINITIONS_GIT_FOLDER ,".git"),
                                "log",
                                '--pretty=%cs|%an|%H',
                                "-L {start},+{length}:{file}".format(start=start, length=length, file=ED_FILENAME))
                                , stdout=subprocess.PIPE)
    output = subprocess.check_output(('head', '-n', '1'), stdin=ps.stdout)
    ps.wait()
    last_modified_date, last_modified_author, last_modified_hash = output.decode('UTF-8')[:-1].split('|') # remove '\n' 
    return last_modified_date, last_modified_author, last_modified_hash


def _get_usage_chart(usage_data: pd.DataFrame, event_name: str) -> str:
    chart = UsageChartGenerator.UsageChartGenerator(event_name)
    weeks = [ week.strftime("%d/%m/%Y") for week in usage_data['WEEK'].tolist()]
    totals = [total if total is not None else 0 for total in usage_data[event_name].tolist()]
    chart.add_data(x_series=weeks, y_series=totals)
    return chart.generate_chart()


def generate_markdown(event_key: str, event: dict, repo: git.Reference.repo, event_file: List, model_defs: dict, usage_data: pd.DataFrame ) -> Dict:
    template_path = os.path.join(MAIN_PATH, EVENT_TEMPLATE)
    with open(template_path, 'r') as file:
        event_md = file.read()
    
    event_data = {}
    event_data['event_name'] = event['name']

    creation_date, event_creation_author, event_creation_hash = _get_created_info(event_key, repo)
    event_creation_link = 'https://github.com/airtasker/airtasker_event_definitions/commit/' + event_creation_hash
    event_data['event_creation_date'] = creation_date
    event_data['event_creation_author'] = event_creation_author
    event_data['event_creation_link'] = event_creation_link

    last_modified_date, last_modified_author, last_modified_hash = _get_last_modified_info(event_key, event_file)
    last_modified_link = 'https://github.com/airtasker/airtasker_event_definitions/commit/' + last_modified_hash
    event_data['last_modified_date'] = last_modified_date
    event_data['last_modified_author'] = last_modified_author
    event_data['last_modified_link'] = last_modified_link

    event_data['event_description'] = event['description']
    event_data['event_id'] = event_key
    event_data['event_category'] = event['category']
    event_data['event_platforms'] = _get_platforms(event)
    event_data['event_additional_parameters'] = event['event_specific_parameters'] if 'event_specific_parameters' in event.keys() else []
    event_data['model_properties'] = _get_model_properties(event, model_defs) 
    if ENABLE_SQL_QUERIES:
        event_md = event_md.replace('{<UsageChart>}', _get_usage_chart(usage_data, event['name']))
    else:
        event_md = event_md.replace('{<UsageChart>}', '')
    event_md = event_md.replace('{<yaml_header>}', yaml.dump(event_data))
    
    return event_md


def fetch_usage_data(event_defs):
    event_names = [event['name'] for _, event in event_defs.items()]
    event_names_str = ', '.join(["'{}'".format(event_name) for event_name in event_names])
    
    query = """ with staging as (
        select date_trunc(week, EVENT_CREATED_UTC_DATE) week,
              EVENT_NAME,
              count(*) totals
       from PROD.RAW.AIRTASKER_EVENT
       where EVENT_NAME in (""" + event_names_str + """)
       and EVENT_CREATED_UTC_DATE >= date_trunc(week, dateadd(year, -1, current_date))
       and EVENT_CREATED_UTC_DATE < date_trunc(week, current_date)
        group by 1, 2)
        select *
        from staging
        pivot(sum(totals) for EVENT_NAME in (""" + event_names_str + """))
      as p
      order by week
    """

    usage_data = sql.SnowflakeQuery().fetch_query(query)
    columns = {"'{}'".format(event_name): event_name for event_name in event_names}
    usage_data = usage_data.rename(columns, axis=1)
    return usage_data

def store_md(events_md: str, event: dict, docs_dir: str):
    file_dir = os.path.join(docs_dir, 'events', event['category'])
    file_name = event['name'] + '.md'
    if not os.path.isdir(file_dir):
        os.makedirs(file_dir)
    with open(os.path.join(file_dir, file_name), 'w') as file:
        file.write(events_md)


def clone_ed_repo():
    if os.path.isdir(EVENT_DEFINITIONS_GIT_FOLDER):
        shutil.rmtree(EVENT_DEFINITIONS_GIT_FOLDER)
    os.makedirs(EVENT_DEFINITIONS_GIT_FOLDER)
    return git.Repo.clone_from(EVENT_DEFINITIONS_GIT, EVENT_DEFINITIONS_GIT_FOLDER)


def main(arg):
    logging.info('Starting event generation script..')
    logging.info('****************************************')
    logging.info('** Step 1: Get all information')
    logging.info('****************************************')
    logging.info('[1/4] Get event_defintions_repo...')
    repo = clone_ed_repo()
    logging.info('[1/4] event_defintions_repo loaded.')
    logging.info('[2/4] Get event_defintions file.')
    event_defs = get_event_definitions()
    logging.info('[3/4] Get model_defintions file.')
    model_defs = get_model_definitions()
    models = clean_model_definitions(model_defs)
    logging.info('[4/4] Fetch usage information...')
    usage_date = None
    if ENABLE_SQL_QUERIES:
        usage_date = fetch_usage_data(event_defs)
    logging.info('[4/4] usage information loaded!')
    event_file = get_event_definitions(yaml_format=False)

    logging.info('****************************************')
    logging.info('** Step 2: Generate and store event files.')
    logging.info('****************************************')
    for event_key, event in event_defs.items():
        logging.info('generating event file for {}'.format(event_key))
        event_data = None
        if ENABLE_SQL_QUERIES:
            event_data = usage_date[['WEEK', event['name']]]
        event_md = generate_markdown(event_key, event, repo, event_file, models, event_data)
        store_md(event_md, event, arg.docs_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser('Script to convert event definitions file into markdown format.')
    parser.add_argument('--docs_dir', type=str,
                        help='path to the folder where the generated docs should be stored. The script will need write access to this folder. Defaults to "./docs/"')
    args = parser.parse_args()
    main(args)

