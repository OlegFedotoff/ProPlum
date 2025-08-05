import os
import configparser
import json
from core.utils import get_current_dir, write_json
from core.config import Config

def new_change(**kwargs):
    set_name = kwargs.get('set')
    if not set_name:
        print("\n!!! Missing parameter <repository>")
        return False

    # Read change_prefix from config.ini
    config = configparser.ConfigParser()
    config_file_path = os.path.join(get_current_dir(), "config.ini")
    if not os.path.exists(config_file_path):
        print(f"!!! Config file not found: {config_file_path}")
        return False
    config.read(config_file_path)

    change_prefix = None
    if 'MAIN' in config and 'change_prefix' in config['MAIN']:
        change_prefix = config['MAIN']['change_prefix']
    
    if not change_prefix:
        print("!!! 'change_prefix' is not defined in the [MAIN] section of config.ini")
        return False

    set_base_dir = os.path.join(os.path.abspath('.'), Config.sets_dir, set_name)
    if not os.path.isdir(set_base_dir):
        print(f"!!! Set directory '{set_name}' does not exist.")
        return False

    changes_dir = os.path.join(set_base_dir, "changes")

    # Create 'changes' directory if it doesn't exist, and add .gitkeep
    if not os.path.isdir(changes_dir):
        print(f"\nCreating 'changes' subdirectory for set '{set_name}'...")
        try:
            os.makedirs(changes_dir)
            with open(os.path.join(changes_dir, ".gitkeep"), 'w') as f:
                pass # Create empty .gitkeep
            print(f"  Created {changes_dir} and .gitkeep")
        except OSError as e:
            print(f"!!! Error creating directory {changes_dir}: {e}")
            return False
    else:
        print(f"\n'changes' subdirectory already exists for set '{set_name}'.")

    # Formulate new change name
    max_num = -1
    for item in os.listdir(changes_dir):
        if item.startswith(change_prefix + "_") and os.path.isdir(os.path.join(changes_dir, item)):
            try:
                num_str = item[len(change_prefix) + 1:]
                num = int(num_str)
                if num > max_num:
                    max_num = num
            except ValueError:
                continue # Ignore directories not matching the pattern
    
    new_change_num = max_num + 1
    new_change_name = f"{change_prefix}_{new_change_num}"
    new_change_path = os.path.join(changes_dir, new_change_name)

    # Create new change directory and files
    print(f"\nCreating new change '{new_change_name}' for set '{set_name}'...")
    try:
        os.makedirs(new_change_path)
        with open(os.path.join(new_change_path, ".gitkeep"), 'w') as f:
            pass # Create empty .gitkeep
        if not write_json(os.path.join(new_change_path, "migration.json"), [{"migration" : "create.sql", "rollback" : "rollback.sql"}]):
            print(f"!!! Error creating migration.json for {new_change_name}")
            return False
        print(f"  Created directory {new_change_path} with .gitkeep and migration.json")
    except OSError as e:
        print(f"!!! Error creating change directory {new_change_path}: {e}")
        return False

    print(f"\nNew change '{new_change_name}' created successfully.")
    return True 