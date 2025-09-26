import os
import configparser
from core.handler import Handler, split_string
from core.utils import get_current_dir, read_json
from core.config import Config


def apply(**kwargs):
    set_name = kwargs.get('set')
    cont = kwargs.get('cont', False)
    if not set_name:
        print("\n!!! Missing parameter <repository>")
        return False

    # Check that environment is dev
    env = kwargs.get('env', 'dev')
    if env != 'dev':
        print(f"\n!!! Apply command can only be used in dev environment. Current environment: {env}")
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
                choice = input("\nEnter the number of the change to apply: ").strip()
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

    print(f"\nApplying change: {selected_change}")
    
    # Execute the change (similar to release execution but only on dev)
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

    print(f"\nApplying change '{selected_change}' to dev environment")
    
    # Use Handler to manage database operations
    handler = Handler(db, set=set_name, env="dev")
    if not handler.init():
        print("!!! Failed to initialize handler")
        return False

    print(f"Applying change in schema <{handler.schema}>")

    # Execute each migration in the change
    for migration in migrations:
        result = _execute_change_migration(db, migration, change_path, handler, cont, set_base_dir)
        if not result:
            print(f"!!! Change application failed")
            if not cont:
                return False

    db.commit()

    print(f"\nChange '{selected_change}' applied successfully")
    return True


def _execute_change_migration(db, migration, change_path, handler, cont, set_dir):
    """Execute a single migration from a change (similar to migrate_file but simplified)"""
    print("    " + "Processing " + migration["migration"])

    source_dir = migration.get("source", "")
    if not source_dir or source_dir == "releases":
        source_dir = change_path
    else:
        source_dir = "/".join([set_dir, source_dir])

    filename = os.path.join(source_dir, migration["migration"])
    
    # Determine encoding based on file extension and schema configuration
    file_extension = os.path.splitext(filename)[1].lower()
    if file_extension == '.hsql':
        file_encoding = "cp1251"
    else:
        file_encoding = handler.schema_encoding

    try:
        with open(filename, encoding=file_encoding) as f:
            migration_data = f.read()
    except UnicodeDecodeError as e:
        print("    " + f"!!! Error decoding file {filename} with encoding {file_encoding}: {e}")
        return False
    except Exception as exc:
        print("    " + f"!!! Error opening file {filename}: {exc}")
        return False

    # Execute SQL statements
    when_error = "continue" if cont else "break"
    when_exists = "skip" if cont else "error"
    
    # Detect YAML by extension or content style
    is_yaml_ext = file_extension in [".yaml", ".yml"]
    yaml_style = handler.is_yaml_style_format(migration_data) if not is_yaml_ext else False

    # Set owner role for current schema in GP database
    if not handler.set_owner_role():
        return False

    
    if is_yaml_ext:
        # Parse typed YAML (type: sql | fw_object)
        import yaml
        try:
            parsed = yaml.safe_load(migration_data)
            if not isinstance(parsed, dict):
                print("    " + "!!! Invalid YAML: expected mapping at root")
                return False
            y_type = (parsed.get('type') or '').strip()
            params = parsed.get('params', {}) or {}
            if y_type == 'sql':
                # Execute sections: all then dev
                seq = []
                if 'all' in params: seq.append(('all', params.get('all')))
                if 'dev' in params: seq.append(('dev', params.get('dev')))
                for sec, content in seq:
                    if content and str(content).strip():
                        if not _execute_change_data_section(db, str(content).strip(), when_exists, when_error, handler):
                            return False
            elif y_type == 'fw_object':
                # Delegate to handler's YAML processor to call fw.f_save_object
                if not handler.process_yaml_fw_object(migration_data, 'dev'):
                    print("    " + f"!!! {handler.error_text}")
                    return False
            else:
                print("    " + f"!!! Unsupported YAML type: {y_type}")
                return False
        except yaml.YAMLError as e:
            print("    " + f"!!! YAML parse error: {e}")
            return False
    elif yaml_style:
        # Parse YAML-style migration data using yaml library
        import yaml
        
        try:
            parsed_data = yaml.safe_load(migration_data)
            
            # Execute sections in the order they appear in the YAML file
            if isinstance(parsed_data, dict):
                for section_name, section_content in parsed_data.items():
                    # Check if we should execute this section
                    should_execute = (section_name.lower() == 'all' or 
                                    section_name.lower() == 'dev' or 
                                    ('dev' in [env.strip().lower() for env in section_name.split(',')]))
                    
                    if should_execute and section_content and str(section_content).strip():
                        if not _execute_change_data_section(db, str(section_content).strip(), when_exists, when_error, handler):
                            return False
            else:
                print("    " + "!!! Invalid YAML format: expected dictionary structure")
                return False
                
        except yaml.YAMLError as e:
            print("    " + f"!!! Error parsing YAML data: {e}")
            return False
    else:
        # Use original logic for non-YAML format
        if not _execute_change_data_section(db, migration_data, when_exists, when_error, handler):
            return False

    db.commit()
    print("    " + "Finished")
    return True


def _execute_change_data_section(db, migration_data, when_exists, when_error, handler):
    """Helper function to execute a single YAML section"""
    for sql in split_string(migration_data):
        sql = sql.strip()
        if sql:
            object_type, object_schema, object_name, description, error_text = db.migrate(sql, when_exists, set=handler.set, env="dev")
            if error_text:
                print("    " + f"!!! SQL execution error: {error_text}")
                if when_error == "break":
                    return False
    return True 