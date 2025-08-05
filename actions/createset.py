import os
import random
import string

from core.utils import get_current_dir, file_exists, write_json, read_json
from core.release import do_releses
from core.config import Config


def create_set(db, set):

    if not set:
        print("\n!!! Missing parameter <set>")
        return False

    print("\nCreating set <%s>" % (set,))


    base_dir = get_current_dir()
    sets_dir = base_dir + "/" + Config.sets_dir
    sets_config = sets_dir + "/sets.json"

    error_text, sets = read_json(sets_config)
    if error_text:
        print("!!!", error_text)
        return False
    if not sets:
        sets = {}

    set_data = sets.get(set, None)
    if not set_data:
        set_data = {"name":"Репозитарий " + set, "comment":"", "quota":""}
        sets[set] = set_data

    hdset_dir = sets_dir + "/hdset"



    if db.db_type == "ORCL":

        # Create schema dev
        schema = set
        success = create_schema(db=db, schema=schema, quota=set_data["quota"])
        if not success:
            print("!!! Error creating schema " + schema)
            return False

        # Create schema test
        schema = set + "_test"
        success = create_schema(db=db, schema=schema, quota=set_data["quota"])
        if not success:
            print("!!! Error creating schema " + schema)
            return False

        # Create schema prod
        schema = set + "_prod"
        success = create_schema(db=db, schema=schema, quota=set_data["quota"])
        if not success:
            print("!!! Error creating schema " + schema)
            return False

    # GP
    else:
        db_name = db.db_dev
        success = create_gp_schema(db=db, schema=set, db_name=db_name, env="dev")
        if not success:
            print("!!! Error creating schema " + set + " in database " + db_name)
            return False
        db_name = db.db_test
        success = create_gp_schema(db=db, schema=set, db_name=db_name, env="dev")
        if not success:
            print("!!! Error creating schema " + set + " in database " + db_name)
            return False
        db_name = db.db_prod
        success = create_gp_schema(db=db, schema=set, db_name=db_name, env="prod")
        if not success:
            print("!!! Error creating schema " + set + " in database " + db_name)
            return False



    #########################################################
    # Create catalog
    set_dir = "/".join([sets_dir, set])
    if not file_exists(set_dir):
        try: 
            os.mkdir(set_dir) 
        except OSError as error: 
            print("!!! Error creating catalog", set_dir, error)
            return False

    # Create .gitkeep file in set directory
    gitkeep_file = "/".join([set_dir, ".gitkeep"])
    try:
        with open(gitkeep_file, 'w') as f:
            pass  # Create empty file
    except OSError as error:
        print("!!! Error creating .gitkeep file in set directory", gitkeep_file, error)
        return False


    #########################################################
    # Create releases subdirectory
    releases_dir = "/".join([set_dir, "releases"])
    if not file_exists(releases_dir):
        try: 
            os.mkdir(releases_dir) 
        except OSError as error: 
            print("!!! Error creating releases catalog", releases_dir, error)
            return False
    
    releases_file = "/".join([releases_dir, "releases.json"])
    if not write_json(releases_file, []):
        print("!!! Error writing to file " + releases_file)
        return False

    #########################################################
    # Create encoding_config.json file
    encoding_config_file = "/".join([set_dir, "encoding_config.json"])
    if not write_json(encoding_config_file, {"default_encoding": "utf-8"}):
        print("!!! Error writing to file " + encoding_config_file)
        return False

    #########################################################
    # Create src subdirectory
    src_dir = "/".join([set_dir, "src"])
    if not file_exists(src_dir):
        try: 
            os.mkdir(src_dir) 
        except OSError as error: 
            print("!!! Error creating src catalog", src_dir, error)
            return False
    
    # Create .gitkeep file in src directory
    gitkeep_file = "/".join([src_dir, ".gitkeep"])
    try:
        with open(gitkeep_file, 'w') as f:
            pass  # Create empty file
    except OSError as error:
        print("!!! Error creating .gitkeep file in src directory", gitkeep_file, error)
        return False

    #########################################################
    # Create changes subdirectory
    changes_dir = "/".join([set_dir, "changes"])
    if not file_exists(changes_dir):
        try: 
            os.mkdir(changes_dir) 
        except OSError as error: 
            print("!!! Error creating changes catalog", changes_dir, error)
            return False
    
    # Create .gitkeep file in changes directory
    gitkeep_file = "/".join([changes_dir, ".gitkeep"])
    try:
        with open(gitkeep_file, 'w') as f:
            pass  # Create empty file
    except OSError as error:
        print("!!! Error creating .gitkeep file in changes directory", gitkeep_file, error)
        return False

    #########################################################
    # Create sets.json file
    if not write_json(sets_config, sets):
        print("!!! Error writing to file " + sets_config)
        return False

    print("Done")
    return True


def create_schema(db, schema, quota):
    print("\nCreating schema " + schema)

    if not quota:
        quota = "UNLIMITED"

    schema_exists_sql = "SELECT 1 FROM all_users WHERE username = :schema"
    if not db.exists(schema_exists_sql, (schema.upper(),)):
        letters = string.ascii_letters
        password = ''.join(random.choice(letters) for i in range(10)) + "_1"
        create_user_sql = f"CREATE USER {schema} IDENTIFIED BY {password} DEFAULT TABLESPACE sandbox QUOTA {quota} ON sandbox"
        error_text = db.execute(create_user_sql)
        if error_text:
            print("  !!! Error creating user: " + error_text)
            return False


    print("  Start schema initialization")
    db.set_current_schema(schema)
    success = do_releses(db, "hdset", is_init=True, is_core=True)
    if not success:
        print("  !!! Initialization terminated")
        return False

    print("  Finished")
    print("\nSchema created")
    return True




def create_gp_schema(db, schema, db_name, env="dev"):
    if not db_name:
        return True

    new_db = db.get_new_db(db_name)

    # Create access pattern
    success, user_reader, user_writer = create_access_pattern(db=new_db, set=schema, db_name=db_name)
    if not success:
        print("!!! Error creating access pattern " + schema + " in database " + db_name)
        return False
    
    owner = user_writer

    print("\nCreating schema " + schema + " in database " + db_name)
    
    schema_exists_sql = "SELECT 1 FROM information_schema.schemata WHERE catalog_name = %s AND schema_name = %s"
    if not new_db.exists(schema_exists_sql, (db_name, schema, )):
        create_schema_sql = f'CREATE SCHEMA "{schema}" AUTHORIZATION "{owner}"'
        error_text = new_db.execute(create_schema_sql)
        if error_text:
            print("  !!! Error creating schema: " + error_text)
            return False
    else:
        print("\nSchema " + schema + " in database " + db_name  + " already esists")
        
        # Check owner
        schema_owner_sql = "SELECT schema_owner FROM information_schema.schemata WHERE catalog_name = %s AND schema_name = %s"
        schema_owner = new_db.get_first_row(schema_owner_sql, (db_name, schema,))[0]
        if schema_owner != owner:
            alter_schema_sql = f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"'
            error_text = new_db.execute(alter_schema_sql)
            if error_text:
                print("  !!! Error setting owner for schema: " + error_text)
                return False

    new_db.commit()

    # Grant privileges to owner role
    error_text = new_db.execute(f'GRANT USAGE, CREATE ON SCHEMA "{schema}" TO role_{schema}_owner')
    if error_text:
        print(f"  !!! Error granting USAGE, CREATE ON SCHEMA {schema} to role_{schema}_owner: " + error_text)
        return False


    error_text = new_db.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA "{schema}" TO role_{schema}_rw')
    if error_text:
        print(f"  !!! Error granting ALL ON ALL TABLES IN SCHEMA {schema}: " + error_text)
        return False

    error_text = new_db.execute(f'GRANT ALL ON SCHEMA "{schema}" TO role_{schema}_rw')
    if error_text:
        print(f"  !!! Error granting ALL ON SCHEMA {schema}: " + error_text)
        return False
    error_text = new_db.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO role_{schema}_ro')
    if error_text:
        print(f"  !!! Error granting USAGE ON SCHEMA {schema}: " + error_text)
        return False

    # DEFAULT PRIVILEGES 
    error_text = new_db.execute(f'ALTER DEFAULT PRIVILEGES FOR USER {schema}_writer IN SCHEMA "{schema}" GRANT ALL ON TABLES TO role_{schema}_rw')
    if error_text:
        print(f"  !!! Error ALTER DEFAULT GRANT ALL TABLES IN SCHEMA {schema}: " + error_text)
        return False
    error_text = new_db.execute(f'ALTER DEFAULT PRIVILEGES FOR USER {schema}_writer IN SCHEMA "{schema}" GRANT ALL ON SEQUENCES TO role_{schema}_rw')
    if error_text:
        print(f"  !!! Error ALTER DEFAULT GRANT ALL TABLES IN SCHEMA {schema}: " + error_text)
        return False
    error_text = new_db.execute(f'ALTER DEFAULT PRIVILEGES FOR USER {schema}_writer IN SCHEMA "{schema}" GRANT SELECT ON TABLES TO role_{schema}_ro')
    if error_text:
        print(f"  !!! Error ALTER DEFAULT GRANT SELECT ON TABLES IN SCHEMA {schema}: " + error_text)
        return False

    new_db.commit()

    print("  Start schema initialization")
    new_db.set_current_schema(schema)
    # Set owner role for current schema in GP database
    if new_db.db_type != "ORCL":
        owner_role = f"role_{schema}_owner"
        check_role_sql = "SELECT 1 FROM pg_roles WHERE rolname = %s"
        error_text, role_exists = new_db.get_first_row(check_role_sql, [owner_role])
        if not error_text and role_exists:
            error_text = new_db.execute(f"SET ROLE {owner_role}")
            if error_text:
                print("!!! Failed to set owner role:", error_text)
                return False

    success = do_releses(new_db, "hdset", is_init=True, is_core=True, env=env)
    if not success:
        print("  !!! Initialization terminated")
        return False

    new_db.commit()
    print("  Finished")
    print("\nSchema created")
    return True



def create_access_pattern(db, set, db_name):
    print("\nCreating access pattern " + set)
    user_reader = f"{set}_reader"
    user_writer = f"{set}_writer"
    password = f"{set}_welcome"

    role_ro = f"role_{set}_ro"
    role_rw = f"role_{set}_rw"
    role_owner = f"role_{set}_owner"
    if not create_role(db, role_ro):
        return False, user_reader, user_writer
    if not create_role(db, role_rw):
        return False, user_reader, user_writer
    if not create_role(db, role_owner):
        return False, user_reader, user_writer

    error_text = db.execute(f"GRANT {role_ro} TO role_ml_ro")
    if error_text:
        print(f"  !!! Error granting role {role_ro}: " + error_text)
        return False, user_reader, user_writer
    error_text = db.execute(f"GRANT {role_rw} TO role_ml_rw")
    if error_text:
        print(f"  !!! Error granting role {role_rw}: " + error_text)
        return False, user_reader, user_writer
    error_text = db.execute(f"GRANT {role_owner} TO role_ml_owner")
    if error_text:
        print(f"  !!! Error granting role {role_owner}: " + error_text)
        return False, user_reader, user_writer

    # grant execute dblink_connect_u
    grant_execute_1 = f"GRANT EXECUTE ON FUNCTION dblink_connect_u(text) TO {role_ro}"
    grant_execute_2 = f"GRANT EXECUTE ON FUNCTION dblink_connect_u(text, text) TO {role_ro}"
    error_text = db.execute(grant_execute_1)
    if error_text:
        print(f"  !!! Error GRANT EXECUTE ON FUNCTION dblink_connect_u(text) TO {role_ro}: " + error_text)
        return False
    error_text = db.execute(grant_execute_2)
    if error_text:
        print(f"  !!! Error GRANT EXECUTE ON FUNCTION dblink_connect_u(text, text) TO {role_ro}: " + error_text)
        return False

    if not create_user(db, user_reader, password, set, db_name):
        return False, user_reader, user_writer
    if not create_user(db, user_writer, password, set, db_name):
        return False, user_reader, user_writer

    error_text = db.execute(f"GRANT role_ml_ro TO {user_reader}")
    if error_text:
        print(f"  !!! Error granting role_ml_ro TO {user_reader}: " + error_text)
        return False, user_reader, user_writer
    error_text = db.execute(f"GRANT role_ml_ro TO {user_writer}")
    if error_text:
        print(f"  !!! Error granting role_ml_ro TO {user_writer}: " + error_text)
        return False, user_reader, user_writer
    error_text = db.execute(f"GRANT {role_rw} TO {user_writer}")
    if error_text:
        print(f"  !!! Error granting {role_rw} TO {user_writer}: " + error_text)
        return False, user_reader, user_writer



    db.commit()
    print("Created")
    return True, user_reader, user_writer    


def create_role(db, role_name):
    if db.db_type != "ORCL":
        role_exists_sql = "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s"
        if db.exists(role_exists_sql, (role_name,)):
            print(f"  Role {role_name} exists")
            return True

    create_role_sql = f"CREATE ROLE {role_name}"
    error_text = db.execute(create_role_sql)
    if error_text:
        print(f"  Error creating role {role_name}: " + error_text)
    else:
        print(f"  Role {role_name} is created")
    return True        


def create_user(db, user_name, password, set, db_name):
    if db.db_type == "ORCL":
        user_exists_sql = "SELECT 1 FROM all_users WHERE username = :user_name"
        user_name = user_name.upper()
    else:
        user_exists_sql = "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s"
        user_name = user_name.lower()
    if not db.exists(user_exists_sql, (user_name,)):
        if db.db_type == "ORCL":
            create_user_sql = f"""CREATE USER {user_name} 
                                     IDENTIFIED BY {password} 
                                     DEFAULT TABLESPACE sandbox"""
        else:
            create_user_sql = f"""CREATE USER {user_name} 
                                     WITH PASSWORD '{password}'"""
        error_text = db.execute(create_user_sql)
        if error_text:
            print(f"  !!! Error creating user {user_name}: " + error_text)
            return False

    if db.db_type == "ORCL":
        error_text = db.execute(f"GRANT CONNECT TO {user_name}")
        if error_text:
            print(f"  !!! Error granting connect to {user_name}: " + error_text)
            return False
    else:
        error_text = grant_connect(db, user_name, db_name)
        if error_text:
            print(f"  !!! Error granting connect to {user_name} on {db_name}: " + error_text)
            return False
        # error_text = grant_connect(db, user_name, db.db_test)
        # if error_text:
        #     print(f"  !!! Error granting connect to {user_name} on {db.db_test}: " + error_text)
        #     return False
        # error_text = grant_connect(db, user_name, db.db_prod)
        # if error_text:
        #     print(f"  !!! Error granting connect to {user_name} on {db.db_prod}: " + error_text)
        #     return False

        # search_path
        set_serach_path_sql = f"alter role {user_name} set search_path = {set}, public"
        error_text = db.execute(set_serach_path_sql)
        if error_text:
            print(f"  !!! Error set search_path for user {user_name}: " + error_text)
            return False

    db.commit()
    print(f"  User {user_name} is created")
    return True            


def grant_connect(db, user_name, db_name):
    error_text = ""
    if db_name:
        error_text = db.execute(f'GRANT CONNECT, TEMPORARY ON DATABASE "{db_name}" TO {user_name}')
    return error_text
