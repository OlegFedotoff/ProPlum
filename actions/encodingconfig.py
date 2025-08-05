import os
import json
import shutil
from core.utils import file_exists, read_json, write_json
from core.config import Config

def _get_actual_release_path(base_set_dir):
    """Determines the correct base path for releases (either set_dir or set_dir/releases)."""
    releases_subdir = os.path.join(base_set_dir, "releases")
    if os.path.isdir(releases_subdir):
        return releases_subdir
    return base_set_dir

def _rename_file_if_confirmed(old_path, new_extension, confirmed):
    file_name, current_extension = os.path.splitext(old_path)
    if confirmed and current_extension.lower() == '.sql' and new_extension.lower() == '.hsql':
        new_path = file_name + new_extension
        try:
            os.rename(old_path, new_path)
            print(f"  Renamed: {os.path.basename(old_path)} -> {os.path.basename(new_path)}")
            return True, os.path.basename(new_path) # Return new filename
        except Exception as e:
            print(f"!!! Error renaming file {old_path}: {e}")
            return False, os.path.basename(old_path)
    return False, os.path.basename(old_path)

def configure_encoding(**kwargs):
    set_name = kwargs.get('set')
    if not set_name:
        print("\n!!! Missing parameter <repository>")
        return False

    set_base_dir = os.path.join(os.path.abspath('.'), Config.sets_dir, set_name)

    if not os.path.isdir(set_base_dir):
        print(f"!!! Set directory '{set_name}' does not exist.")
        return False

    # Part 1: Create encoding_config.json if not exists
    encoding_config_path = os.path.join(set_base_dir, "encoding_config.json")
    if not file_exists(encoding_config_path):
        print(f"\nCreating encoding_config.json for set '{set_name}'...")
        encoding_data = {"default_encoding": "utf-8"}
        if not write_json(encoding_config_path, encoding_data):
            print(f"!!! Error creating encoding_config.json at {encoding_config_path}")
            return False
        print(f"  Created {encoding_config_path} with default_encoding: utf-8")
    else:
        print(f"\nencoding_config.json already exists for set '{set_name}'. Nothing to do.")

    # Part 2: Change .sql to .hsql in releases
    print("\nChecking release files for .sql extension...")
    releases_path = _get_actual_release_path(set_base_dir)
    release_dirs = [d for d in os.listdir(releases_path) if os.path.isdir(os.path.join(releases_path, d)) and d.isdigit() and len(d) == 6]

    rename_releases_sql_to_hsql = False
    if release_dirs:
        response = input(f"Do you want to rename ALL .sql files to .hsql in releases for set '{set_name}' (y/n)? ")
        if response.lower() == 'y':
            rename_releases_sql_to_hsql = True

    for release_code in sorted(release_dirs):
        release_dir = os.path.join(releases_path, release_code)
        migration_json_path = os.path.join(release_dir, "migration.json")
        
        error, migrations_data = read_json(migration_json_path)
        if error or not migrations_data:
            print(f"!!! Warning: Could not read or parse migration.json for release {release_code}. Skipping.")
            continue

        migrations_changed = False
        for migration_entry in migrations_data:
            # Process migration file
            migration_file = migration_entry.get("migration")
            if migration_file:
                old_migration_path = os.path.join(release_dir, migration_file)
                renamed, new_migration_file = _rename_file_if_confirmed(old_migration_path, ".hsql", rename_releases_sql_to_hsql)
                if renamed:
                    migration_entry["migration"] = new_migration_file
                    migrations_changed = True

            # Process rollback file
            rollback_file = migration_entry.get("rollback")
            if rollback_file:
                old_rollback_path = os.path.join(release_dir, rollback_file)
                renamed, new_rollback_file = _rename_file_if_confirmed(old_rollback_path, ".hsql", rename_releases_sql_to_hsql)
                if renamed:
                    migration_entry["rollback"] = new_rollback_file
                    migrations_changed = True
        
        if migrations_changed:
            if not write_json(migration_json_path, migrations_data):
                print(f"!!! Error writing updated migration.json for release {release_code}")
            else:
                print(f"  Updated migration.json for release {release_code}")

    print("Finished checking release files.")

    # Part 3: Change .sql to .hsql in src subdirectory
    src_dir = os.path.join(set_base_dir, "src")
    if os.path.isdir(src_dir):
        print(f"\nChecking files in src subdirectory '{src_dir}' for .sql extension...")
        
        rename_src_sql_to_hsql = False
        response = input(f"Do you want to rename ALL .sql files to .hsql in src subdirectory for set '{set_name}' (y/n)? ")
        if response.lower() == 'y':
            rename_src_sql_to_hsql = True

        for root, dirs, files in os.walk(src_dir):
            for file in files:
                if file.lower().endswith('.sql'):
                    old_path = os.path.join(root, file)
                    _rename_file_if_confirmed(old_path, ".hsql", rename_src_sql_to_hsql)
        print("Finished checking src files.")
    else:
        print(f"\nNo 'src' subdirectory found for set '{set_name}'. Skipping.")

    print("\nEncoding configuration complete.")
    return True 