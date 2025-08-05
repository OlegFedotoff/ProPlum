import os
import shutil
import configparser
from core.handler import Handler
from core.utils import get_current_dir, read_json, write_json
from core.config import Config


def new_release(db, set):

    if not set:
        print("\n!!! Missing parameter <repository>")
        return False

    handler = Handler(db, set)
    if not handler.init():
        return False

    if set == "hdset":
        print("\nCreating new core release")
        # For core releases, always create empty release
        if not handler.create_new_release():
            return False
        print("Done")
        return True
    else:
        print("\nCreating new release for repository <%s>" % (set,))

    # Ask user about release type
    while True:
        try:
            choice = input("\nChoose release type:\n  1. Empty release\n  2. Release based on change\n\nEnter your choice (1 or 2): ").strip()
            if choice == "1":
                # Create empty release (original behavior)
                if not handler.create_new_release():
                    return False
                print("Done")
                return True
            elif choice == "2":
                # Create release based on change
                return _create_release_from_change(handler, set)
            else:
                print("!!! Please enter 1 or 2")
        except KeyboardInterrupt:
            print("\n!!! Operation cancelled by user")
            return False


def _create_release_from_change(handler, set_name):
    """Create a new release based on an existing change"""
    
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
    changes_dir = os.path.join(set_base_dir, "changes")
    
    if not os.path.isdir(changes_dir):
        print(f"!!! Changes directory does not exist for set '{set_name}'. Use 'newchange' command to create changes.")
        return False

    # Find all changes matching the prefix
    changes = []
    for item in os.listdir(changes_dir):
        if item.startswith(change_prefix + "_") and os.path.isdir(os.path.join(changes_dir, item)):
            changes.append(item)
    
    if not changes:
        print(f"!!! No changes found with prefix '{change_prefix}' in set '{set_name}'")
        return False

    # Sort changes by number
    changes.sort(key=lambda x: int(x.split('_')[-1]) if x.split('_')[-1].isdigit() else 0)

    selected_change = None
    if len(changes) == 1:
        selected_change = changes[0]
        print(f"\nFound one change: {selected_change}")
    else:
        print(f"\nFound {len(changes)} changes:")
        for i, change in enumerate(changes, 1):
            print(f"  {i}. {change}")
        
        while True:
            try:
                choice = input("\nEnter the number of the change to create release from: ").strip()
                choice_num = int(choice)
                if 1 <= choice_num <= len(changes):
                    selected_change = changes[choice_num - 1]
                    break
                else:
                    print(f"!!! Please enter a number between 1 and {len(changes)}")
            except ValueError:
                print("!!! Please enter a valid number")
            except KeyboardInterrupt:
                print("\n!!! Operation cancelled by user")
                return False

    print(f"\nCreating release from change: {selected_change}")
    
    # Get new release code
    new_release_code = "000001"
    if handler.releases:
        last_release = handler.get_last_release()
        new_release_code = last_release["code"]
        new_release_int = int(new_release_code) + 1
        new_release_code = str(new_release_int).rjust(6, "0")

    # Create new release entry
    new_release = {"code": new_release_code, "name": "", "comment": f"Created from change {selected_change}"}
    handler.releases.append(new_release)

    # Create release directory
    set_releases_dir = handler._get_actual_release_path(handler.set_dir)
    release_dir = os.path.join(set_releases_dir, new_release_code)
    change_path = os.path.join(changes_dir, selected_change)
    
    try:
        # Copy entire change directory content to release directory
        shutil.copytree(change_path, release_dir)
        print(f"  Copied content from {selected_change} to release {new_release_code}")
        
        # Remove .gitkeep file from release directory if it exists
        gitkeep_path = os.path.join(release_dir, ".gitkeep")
        if os.path.exists(gitkeep_path):
            os.remove(gitkeep_path)
            
    except Exception as error:
        print(f"!!! Error copying change content: {error}")
        return False

    # Update releases.json
    if not write_json(handler.release_filename, handler.releases):
        print(f"!!! Error writing to file {handler.release_filename}")
        return False

    # Remove the change directory
    try:
        shutil.rmtree(change_path)
        print(f"  Removed change directory {selected_change}")
    except Exception as error:
        print(f"!!! Error removing change directory: {error}")
        return False

    print(f"\nRelease {new_release_code} created successfully from change {selected_change}")
    print("Done")
    return True



    # if not set:
    #     print("\n!!! Missing parameter <set>")
    #     return False

    # print("\nCreating new release for repository <%s>" % (set,))

    # base_dir = get_current_dir()
    # sets_dir = base_dir + "/sets"
    # sets_config = sets_dir + "/sets.json"

    # error_text, sets = read_json(sets_config)
    # if error_text:
    #     print("!!!", error_text)
    #     return False
    # if not sets:
    #     sets = {}

    # set_data = sets.get(set, None)
    # if not set_data:
    #     print("!!!", f"Unknown repository {set}")
    #     return False

    # set_dir = "/".join([sets_dir, set])
    # release_filename = "/".join([set_dir, "releases.json"])
    # error_text, releases = read_json(release_filename)
    # if error_text:
    #     print("!!!", error_text)
    #     return False

    # new_release_code = "000001"
    # if releases:
    #    new_release_code = sorted(releases, key=lambda x: x['code'], reverse=True)[0]["code"]
    #    new_release_int = int(new_release_code) + 1
    #    new_release_code = str(new_release_int).rjust(6, "0")

    # new_release = {"code":new_release_code, "name":"", "comment":""}
    # releases.append(new_release)


    # # Create catalog
    # release_dir = "/".join([set_dir, new_release_code])
    # try: 
    #     os.mkdir(release_dir) 
    # except OSError as error: 
    #     print("!!! Error creating catalog", release_dir, error)
    #     return False
    # migration_file = "/".join([release_dir, "migration.json"])
    # empty_migrtion = [{"migration" : "create.sql", "rollback" : "rollback.sql"}]
    # if not write_json(migration_file, empty_migrtion):
    #     print("!!! Error writing to file " + migration_file)
    #     return False


    # if not write_json(release_filename, releases):
    #     print("!!! Error writing to file " + release_filename)
    #     return False

    # print("Done")
    # return True
