import os
import json

def get_current_dir():
    # Get the path to the main directory where manage.py is located
    current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return current_dir

def file_exists(file_name):
    return os.path.exists(file_name)


def write_json(json_file, json_data, encoding='UTF-8'):
    try:
        with open(json_file, 'w', encoding=encoding) as f:
            f.write(json.dumps(json_data, ensure_ascii=False, indent=4))
    except Exception as e:
        return False
    return True

def read_json(json_file, encoding='UTF-8'):
    data = None
    if not file_exists(json_file):
        return "", data
    try:
        with open(json_file, encoding=encoding) as f: 
            data = json.load(f)
    except json.decoder.JSONDecodeError as e:
        return  "Error reading " + json_file + " : " + str(e), data
    return "", data

