import os
import configparser
from core.handler import Handler, split_string
from core.utils import get_current_dir, read_json
from core.config import Config


def rollback_change(**kwargs):
    set_name = kwargs.get('set')
    cont = kwargs.get('cont', False)
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
    if not os.path.isdir(changes_dir):
        print(f"!!! Changes directory does not exist for set '{set_name}'. No changes to rollback.")
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
                choice = input("\nEnter the number of the change to rollback: ").strip()
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

    print(f"\nRolling back change: {selected_change}")
    
    # Execute the rollback (rollback files from the change)
    change_path = os.path.join(changes_dir, selected_change)
    migration_file = os.path.join(change_path, "migration.json")
    
    if not os.path.exists(migration_file):
        print(f"!!! Migration file not found: {migration_file}")
        return False

    error_text, migrations = read_json(migration_file)
    if error_text:
        print(f"!!! Error reading migration file: {error_text}")
        return False
    
    if not migrations:
        print(f"!!! Empty migration file: {migration_file}")
        return False

    # Initialize database connection - always use dev environment
    db = kwargs.get('db')
    if not db:
        print("!!! Database connection not provided")
        return False

    print(f"\nRolling back change '{selected_change}' in dev environment")
    
    # Use Handler to manage database operations
    handler = Handler(db, set=set_name, env="dev")
    if not handler.init():
        print("!!! Failed to initialize handler")
        return False

    print(f"Rolling back change in schema <{handler.schema}>")

    # Execute rollback for each migration in the change (in reverse order)
    success = True
    for migration in reversed(migrations):
        result = _execute_change_rollback(db, migration, change_path, handler, cont)
        if not result:
            print(f"!!! Change rollback failed")
            success = False
            if not cont:
                return False

    db.commit()

    if success:
        print(f"\nChange '{selected_change}' rolled back successfully")
    else:
        print(f"\nChange '{selected_change}' rollback completed with errors")
    return True


def _execute_change_rollback(db, migration, change_path, handler, cont=False):
    """Execute rollback for a single migration from a change"""
    rollback_file = migration.get("rollback", "")
    if not rollback_file:
        print("    " + f"No rollback file specified for migration {migration.get('migration', 'unknown')}")
        return True

    print("    " + "Rolling back " + rollback_file)

    rollback_filename = os.path.join(change_path, rollback_file)
    
    if not os.path.exists(rollback_filename):
        print("    " + f"!!! Rollback file not found: {rollback_filename}")
        return False

    # Determine encoding based on file extension and schema configuration
    file_extension = os.path.splitext(rollback_filename)[1].lower()
    if file_extension == '.hsql':
        file_encoding = "cp1251"
    else:
        file_encoding = handler.schema_encoding

    try:
        with open(rollback_filename, encoding=file_encoding) as f:
            rollback_data = f.read()
    except UnicodeDecodeError as e:
        print("    " + f"!!! Error decoding rollback file {rollback_filename} with encoding {file_encoding}: {e}")
        return False
    except Exception as exc:
        print("    " + f"!!! Error opening rollback file {rollback_filename}: {exc}")
        return False

    rollback_data = rollback_data.strip()
    if not rollback_data:
        print("    " + "Empty rollback file, skipping")
        return True

    # Execute rollback SQL statements
    when_error = "continue" if cont else "break"
    when_exists = "skip" if cont else "error"
    
    # Check if this is a new format file (YAML-style with environment sections)
    yaml_style = handler.is_yaml_style_format(rollback_data)
    
    if yaml_style:
        # Parse YAML-style rollback data using yaml library
        import yaml
        
        try:
            parsed_data = yaml.safe_load(rollback_data)
            
            # Execute sections in the order they appear in the YAML file
            if isinstance(parsed_data, dict):
                for section_name, section_content in parsed_data.items():
                    # Check if we should execute this section
                    should_execute = (section_name.lower() == 'all' or 
                                    section_name.lower() == 'dev' or 
                                    ('dev' in [env.strip().lower() for env in section_name.split(',')]))
                    
                    if should_execute and section_content and str(section_content).strip():
                        print("    " + f"Rolling back section: {section_name}")
                        if not _execute_rollback_data_section(db, str(section_content).strip(), when_exists, when_error, handler):
                            return False
            else:
                print("    " + "!!! Invalid YAML format in rollback: expected dictionary structure")
                return False
                
        except yaml.YAMLError as e:
            print("    " + f"!!! Error parsing YAML rollback data: {e}")
            return False
    else:
        # Use original logic for non-YAML format
        if not handler.rollback_data(rollback_data, when_exists, when_error):
            return False

    print("    " + "Finished")
    return True


def _execute_rollback_data_section(db, rollback_data, when_exists, when_error, handler):
    """Helper function to rollback a single YAML section"""
    for sql in split_string(rollback_data):
        sql = sql.strip()
        if sql:
            object_type, object_schema, object_name, description, error_text = db.migrate(sql, when_exists, set=handler.set, env="dev")
            if error_text:
                print("    " + f"!!! Rollback SQL execution error: {error_text}")
                if when_error == "break":
                    return False
    return True 