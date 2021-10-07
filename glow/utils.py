import yaml
import os

def get_file(file_path, yaml_format=False):
    if not os.path.isfile(file_path):
        #raise FileNotFoundError
        pass
    with open(file_path, 'r') as file:
        if yaml_format:
            result = yaml.safe_load(file)
        else:
            result = file.readlines()
    return result

def store_md(md_file, def_type, def_category, def_name, docs_dir):
    file_dir = os.path.join(docs_dir, def_type, def_category)
    file_name = def_name + '.md'
    if not os.path.isdir(file_dir):
        os.makedirs(file_dir)
    with open(os.path.join(file_dir, file_name), 'w') as file:
        file.write(md_file)