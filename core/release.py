import os
from core.utils import get_current_dir, read_json
from core.handler import Handler
from core.config import Config


##########################################################  
def get_actual_release_path(base_set_dir):
    """Determines the correct base path for releases (either set_dir or set_dir/releases)."""
    releases_subdir = os.path.join(base_set_dir, "releases")
    if os.path.isdir(releases_subdir):
        return releases_subdir
    return base_set_dir


def do_releses(db, set_code, is_init=False, is_core=False, env="dev"):
    start_release = "000000"
    if not is_init:
        pass

    set_dir = "/".join([get_current_dir(), Config.sets_dir, set_code])
    set_releases_dir = get_actual_release_path(set_dir)
    release_filename = "/".join([set_releases_dir, "releases.json"])
    error_text, releases = read_json(release_filename)
    if error_text:
        print(" "*2 + "!!! " + error_text)
        return False
    if not releases:
        print(" "*2 + "!!! Empty release file " + release_filename)
        return False

    releases = [r for r in releases if r["code"] > start_release]
    releases = sorted(releases, key=lambda x: x['code'])
    for release in releases:
        result = do_release(db, release, set_dir, is_core, env=env)
        if not result:
            return False
    return True

def do_release(db, release, set_dir, is_core=False, env="dev"):
    print(" "*4 + release["code"])

    set_releases_dir = get_actual_release_path(set_dir)
    release_dir = "/".join([set_releases_dir, release["code"]])
    migration_filename = "/".join([release_dir, "migration.json"])
    
    error_text, migrations = read_json(migration_filename)
    if error_text:
        print(" "*4 + "!!! " + error_text)
        return False
    if not migrations:
        print(" "*4 + "!!! Empty migration file " + migration_filename)
        return False

    for migration in migrations:
        result = migrate(db, migration, release_dir, env=env)
        if not result:
            print(" "*4 + "!!! Release terminates")
            return False

    # update release version
    if db.db_type == "ORCL":
        update_version_sql = "UPDATE hdset_params SET value = :version WHERE code = :code"
    else:
        update_version_sql = "UPDATE hdset_params SET value = %s WHERE code = %s"
    if is_core:
        code = "core_version"
    else:
        code = "migration_version"
    parameters = [release["code"], code,]
    error_text = db.execute(update_version_sql, parameters)
    if error_text:
        print(" "*4 + "!!! Error change version: " + error_text)
        return False

    db.commit()


    print(" "*4 + "End " + release["code"])
    return True


def migrate(db, migration, release_dir, env="dev"):
    print(" "*6 + "Processing " + migration["migration"])

    filename = "/".join([release_dir, migration["migration"]])
    when_error = migration.get("when-error", "break")
    when_exists = migration.get("when-exists", "error")
    db_type = migration.get("db-type", "")

    if db_type and db_type != db.db_type:
        print(" "*6 + "Skip")
        return True

    with open(filename, encoding='cp1251') as f:
        data = f.read()
    for sql in data.split(";"):
        sql = sql.strip()
        result = ""
        if sql:
            object_type, object_schema, object_name, description, result = db.migrate(sql, when_exists, env=env)
            if result and (when_error == "break"):
                print(" "*6 + "!!! Process terminated")
                return False

    print(" "*6 + "Finished")
    return True
