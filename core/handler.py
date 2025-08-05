import os

from core.utils import get_current_dir, file_exists, write_json, read_json
from core.config import Config


def split_string(input_string):
    result = []
    temp = ''
    ignore = False
    ignore1 = False
    for char in input_string:
        if char == '$' and not ignore and not ignore1:
            ignore1 = True
            temp += char
        elif char == '$' and not ignore and ignore1:
            ignore1 = False
            ignore = True
            temp += char
        elif char == '$' and ignore and not ignore1:
            ignore1 = True
            temp += char
        elif char == '$' and ignore and ignore1:
            ignore = False
            ignore1 = False
            temp += char
        elif char == ';' and not ignore:
            result.append(temp)
            temp = ''
            ignore1 = False
        else:
            temp += char
            ignore1 = False
    result.append(temp)
    return result


class Handler(object):

    CORE_VERSION      = "core_version"
    MIGRATION_VERSION = "migration_version"

    ACTION_MIGRATE  = "M"
    ACTION_ROLLBACK = "R"
    ACTION_OTHER    = "O"

    STATUS_SUCCESS  = "S"
    STATUS_ERROR    = "E"
    STATUS_INSERT   = "I"
    STATUS_DELETE   = "D"
    STATUS_ROLLBACK = "R"

    def __init__(self, db, set, env=None):
        super(Handler, self).__init__()
        self.action_type = self.ACTION_OTHER
        self.db = db
        self.set = set
        self.env = env
        self.is_core = (set=="hdset")
        self.schema = ""
        self.base_dir = get_current_dir()
        self.sets_dir = self.base_dir + "/" + Config.sets_dir
        self.sets_config = self.sets_dir + "/sets.json"
        self.error_text = ""
        self.sets = {}
        self.set_data = {}
        self.set_dir = ""
        self.release_filename = ""
        self.releases = []
        self.core_dir = ""
        self.core_release_filename = ""
        self.core_releases = []
        self.release_dir = ""
        self.migration_filename = ""
        self.release_id = None
        self.migration_id = None
        self.schema_encoding = "cp1251" # Default encoding for schema files

    ##########################################################  
    def init(self):

        self.error_text, self.sets = read_json(self.sets_config)
        if self.error_text:
            print("!!!", self.error_text)
            return False
        if not self.sets:
            self.sets = {}

        self.core_dir = "/".join([self.sets_dir, "hdset"])
        core_releases_dir = self._get_actual_release_path(self.core_dir)
        self.core_release_filename = "/".join([core_releases_dir, "releases.json"])
        self.error_text, self.core_releases = read_json(self.core_release_filename)
        if self.error_text:
            print("!!!", self.error_text)
            return False

        if not self.set:
            return True

        if self.set != "hdset":
            self.set_data = self.sets.get(self.set, None)
            if not self.set_data:
                print("!!!", f"Unknown repository {self.set}")
                return False


        self.set_dir = "/".join([self.sets_dir, self.set])
        set_releases_dir = self._get_actual_release_path(self.set_dir)
        self.release_filename = "/".join([set_releases_dir, "releases.json"])
        self.error_text, self.releases = read_json(self.release_filename)
        if self.error_text:
            print("!!!", self.error_text)
            return False

        # Read encoding configuration for the current set
        encoding_config_path = os.path.join(self.set_dir, "encoding_config.json")
        error, encoding_data = read_json(encoding_config_path)
        if not error and encoding_data and "default_encoding" in encoding_data:
            self.schema_encoding = encoding_data["default_encoding"]
            print(f"Using schema encoding '{self.schema_encoding}' for set '{self.set}'")
        else:
            print(f"No custom encoding config found for set '{self.set}'. Using default 'cp1251'.")

        if self.env:
            self.schema = self.set
            if self.env != "dev" and self.db.db_type == "ORCL":
                self.schema += "_" + self.env
            self.error_text = self.db.set_current_schema(self.schema)
            if self.error_text:
                print("!!!", self.error_text)
                return False

        return True

    ##########################################################  
    def get_last_release(self, is_core=False, release_code=None):
        releases = self.core_releases if is_core else self.releases
        if release_code:
            releases = [r for r in releases if r['code'] < release_code]
        if releases:
            releases = sorted(releases, key=lambda x: x['code'], reverse=True)[0]
        return releases


    ##########################################################  
    def migrate(self, is_core=False, start_release="", cont=False):
        self.is_core = is_core
        self.action_type = self.ACTION_MIGRATE
        is_force = True if start_release else False
        if not start_release:
            self.error_text, start_release = self.get_db_version(is_core)
            if self.error_text:
                print("!!!", self.error_text)
                return False

        releases = self.core_releases if self.is_core else self.releases
        release_filename = self.core_release_filename if self.is_core else self.release_filename
        if not releases:
            print("!!! Empty release file " + release_filename)
            return False

        if is_force:
            releases = [r for r in releases if r["code"] >= start_release]
        else:
            releases = [r for r in releases if r["code"] > start_release]
        if not releases:
            print("Nothing to do. Repository is up to date.")
            return True
        releases = sorted(releases, key=lambda x: x['code'])


        # Set owner role for current schema in GP database
        if self.db.db_type != "ORCL":
            owner_role = f"role_{self.set}_owner"
            check_role_sql = "SELECT 1 FROM pg_roles WHERE rolname = %s"
            self.error_text, role_exists = self.db.get_first_row(check_role_sql, [owner_role])
            if not self.error_text and role_exists:
                self.error_text = self.db.execute(f"SET ROLE {owner_role}")
                if self.error_text:
                    print("!!! Failed to set owner role:", self.error_text)
                    return False

        for release in releases:
            result = self.do_release(release, cont=cont)
            if not result:
                return False
        return True


    ##########################################################  
    def rollback(self, release_code, cont=False):
        self.is_core = False
        self.action_type = self.ACTION_ROLLBACK

        self.error_text, current_release = self.get_db_version(is_core=False)
        if self.error_text:
            print("!!!", self.error_text)
            return False

        if release_code != current_release:
            print("!!! Can't rollback release %s. Last installed release: %s" % (release_code, current_release))
            return False

        relese_status = "'R','S','E'" if cont else "'S'"
        if self.db.db_type == "ORCL":
            select_release = f"SELECT release_id FROM hdset_releases WHERE release_code=:code AND status IN({relese_status}) ORDER BY release_id DESC"
        else:
            select_release = f"SELECT release_id FROM hdset_releases WHERE release_code=%s AND status IN({relese_status}) ORDER BY release_id DESC"
        self.error_text, release_data = self.db.get_first_row(select_release, [release_code,])

        if self.error_text:
            print("!!! Error selecting release:", self.error_text)
            return False
        if not release_data:
            print(f"!!! Release {release_code} is not installed")
            return False

        self.release_id = release_data[0]
        if self.db.db_type == "ORCL":
            select_migrations = "SELECT migration_id, rollback_file, rollback_data FROM hdset_migrations WHERE release_id=:release_id ORDER BY migration_id DESC"
        else:
            select_migrations = "SELECT migration_id, rollback_file, rollback_data FROM hdset_migrations WHERE release_id=%s ORDER BY migration_id DESC"

        self.error_text, migrations = self.db.get_all_rows(select_migrations, [self.release_id,])
        if self.error_text:
            print("!!! Error selecting migrations:", self.error_text)
            return False
        if not release_data:
            print(f"!!! Release {release_code} has not any migration")
            return False

        print(" "*4 + "Start rollback" + release_code)
        for migration in migrations:
            self.migration_id = migration[0]
            success = self.rollback_file(migration[1], migration[2], cont=cont)
            if not success:
                self._finish_release(status=self.STATUS_ERROR, error_text=self.error_text)
                print(" "*4 + "!!! Rollback terminates")
                return False

        # update release version
        last_release = self.get_last_release(release_code=release_code)
        last_release_code = "000000" if not last_release else last_release["code"]
        if self.db.db_type == "ORCL":
            update_version_sql = "UPDATE hdset_params SET value = :version WHERE code = :code"
        else:
            update_version_sql = "UPDATE hdset_params SET value = %s WHERE code = %s"
        code = self.CORE_VERSION if self.is_core else self.MIGRATION_VERSION
        parameters = [last_release_code, code,]
        error_text = self.db.execute(update_version_sql, parameters)
        if error_text:
            self._finish_release(status=self.STATUS_ERROR, error_text="Error change version: " + error_text)
            print(" "*4 + "!!! Error change version: " + error_text)
            return False

        self.db.commit()

        self._finish_release(status=self.STATUS_ROLLBACK)
        print(" "*4 + "Rollback finished")


        return True

    ##########################################################  
    def do_release(self, release, cont=False):
        print(" "*4 + release["code"])

        # Deleting old releases
        if self.db.db_type == "ORCL":
            sql = "SELECT release_id FROM hdset_releases WHERE release_code=:code AND status='S' ORDER BY release_id"
        else:
            sql = "SELECT release_id FROM hdset_releases WHERE release_code=%s AND status='S' ORDER BY release_id"
        self.error_text, old_releases = self.db.get_all_rows(sql, [release["code"],])
        if self.error_text:
            print(" "*4 + "!!! Error find old releases:" + self.error_text)
            return False
        if old_releases:
            for r in old_releases:
                self.error_text = self._delete_release(release_id = r[0])
                if self.error_text:
                    print(" "*4 + "!!! Error deleting old releases:" + self.error_text)
                    return False

        self.error_text = self._insert_release(release)
        if self.error_text:
            print(" "*4 + "!!! " + self.error_text)
            return False

        self.error_text, migrations = self.read_migration(release["code"])

        if self.error_text:
            self._finish_release(status=self.STATUS_ERROR, error_text=self.error_text)
            print(" "*4 + "!!! " + self.error_text)
            return False
        if not migrations:
            self.error_text = "Empty migration file " + self.migration_filename 
            self._finish_release(status=self.STATUS_ERROR, error_text=self.error_text)
            print(" "*4 + "!!! " + self.error_text)
            return False

        for migration in migrations:
            result = self.migrate_file( migration, self.release_dir, cont=cont)
            if not result:
                self._finish_release(status=self.STATUS_ERROR, error_text=self.error_text)
                print(" "*4 + "!!! Release terminates")
                return False

        # update release version
        if self.db.db_type == "ORCL":
            update_version_sql = "UPDATE hdset_params SET value = :version WHERE code = :code"
        else:
            update_version_sql = "UPDATE hdset_params SET value = %s WHERE code = %s"
        code = self.CORE_VERSION if self.is_core else self.MIGRATION_VERSION
        parameters = [release["code"], code,]
        error_text = self.db.execute(update_version_sql, parameters)
        if error_text:
            self._finish_release(status=self.STATUS_ERROR, error_text="Error change version: " + error_text)
            print(" "*4 + "!!! Error change version: " + error_text)
            return False

        self.db.commit()

        self._finish_release(status=self.STATUS_SUCCESS)
        print(" "*4 + "End " + release["code"])
        return True



    ##########################################################  
    def is_yaml_style_format(self, data):
        """Check if data is in YAML-style format with environment sections"""
        yaml_style = False
        for line in data.strip().split('\n'):
            line = line.strip()
            if line and ':' in line:
                env_part = line.split(':')[0].strip()
                # Check if it matches known environment patterns (single or comma-separated)
                env_keywords = {'all', 'dev', 'prod', 'test'}
                if env_part in env_keywords or any(env.strip() in env_keywords for env in env_part.split(',')):
                    yaml_style = True
                    break
        return yaml_style

    ##########################################################  
    def migrate_file(self, migration, release_dir, cont=False):
        print(" "*6 + "Processing " + migration["migration"])

        source_dir = migration.get("source", "")
        if not source_dir or source_dir == "releases":
            source_dir = release_dir
        else:
            set_dir = self.core_dir if self.is_core else self.set_dir
            source_dir = "/".join([set_dir, source_dir])

        rollback_dir = migration.get("rollback-source", "")
        if not rollback_dir or rollback_dir == "releases":
            rollback_dir = release_dir
        else:
            set_dir = self.core_dir if self.is_core else self.set_dir
            rollback_dir = "/".join([set_dir, rollback_dir])

        filename = "/".join([source_dir, migration["migration"]])
        rollback_filename = migration["rollback"]
        if rollback_filename:
            rollback_filename = "/".join([rollback_dir, rollback_filename])
        rollback_data = ""
        # when_error = migration.get("when-error", "break")
        # when_exists = migration.get("when-exists", "error")
        if cont:
            when_error = "continue"
            when_exists = "skip"
        else:
            when_error = "break"
            when_exists = "error"
        self.error_text = ""

        # Determine encoding based on file extension and schema configuration
        file_extension = os.path.splitext(filename)[1].lower()
        if file_extension == '.hsql':
            file_encoding = "cp1251"
        else:
            file_encoding = self.schema_encoding # Use schema-specific encoding

        try:
            with open(filename, encoding=file_encoding) as f:
                migration_data = f.read()
        except UnicodeDecodeError as e:
            self.error_text = f"Error decoding file {filename} with encoding {file_encoding}: {e}"
            print(" "*6 + "!!! " + self.error_text)
            return False
        except Exception as exc:
            self.error_text = "Error open file " + filename
            print(" "*6 + "!!! " + self.error_text)
            print(exc)
            return False



        # Check if this is a new format file (YAML-style with environment sections)
        yaml_style = self.is_yaml_style_format(migration_data)


        if rollback_filename:
            try:
                with open(rollback_filename, encoding=file_encoding) as f: # Use determined encoding for rollback file as well
                    rollback_data = f.read()
            except UnicodeDecodeError as e:
                self.error_text = f"Error decoding rollback file {rollback_filename} with encoding {file_encoding}: {e}"
                print(" "*6 + "!!! " + self.error_text)
                return False
            except Exception as exc:
                self.error_text = "Error open rollback file " + rollback_filename
                print(" "*6 + "!!! " + self.error_text)
                print(exc)
                return False

        self._insert_migration(migration, migration_data, rollback_data)

        if yaml_style:
            # Parse YAML-style migration data using yaml library
            import yaml
            
            try:
                parsed_data = yaml.safe_load(migration_data)
                
                # Execute sections in the order they appear in the YAML file
                if isinstance(parsed_data, dict):
                    for section_name, section_content in parsed_data.items():
                        # Check if we should execute this section
                        should_execute = (section_name.lower() == 'all' or 
                                        (self.env and section_name.lower() == self.env.lower()) or 
                                        (self.env and self.env.lower() in [env.strip().lower() for env in section_name.split(',')]))
                        
                        if should_execute and section_content and str(section_content).strip():
                            print(" "*6 + f"Executing section: {section_name}")
                            if not self.migrate_data(str(section_content).strip(), when_exists, when_error):
                                return False
                else:
                    self.error_text = "Invalid YAML format: expected dictionary structure"
                    print(" "*6 + "!!! " + self.error_text)
                    return False
                    
            except yaml.YAMLError as e:
                self.error_text = f"Error parsing YAML data: {e}"
                print(" "*6 + "!!! " + self.error_text)
                return False


        else:
            if not self.migrate_data(migration_data, when_exists, when_error):
                return False


        self._finish_migration(status=self.STATUS_SUCCESS)
        print(" "*6 + "Finished")
        return True


    ##########################################################  
    def migrate_data(self, migration_data, when_exists="error", when_error="break"):
        self.error_text = ""
        for sql in split_string(migration_data):
            sql = sql.strip()
            if sql:
                object_type, object_schema, object_name, description, self.error_text = self.db.migrate(sql, when_exists, set=self.set, env=self.env)
                self._sql_logging(object_type, object_schema, object_name, description, self.error_text)
                if self.error_text and (when_error == "break"):
                    print(" "*6 + "!!! Process terminated")
                    self._finish_migration(status=self.STATUS_ERROR, error_text=self.error_text)
                    return False
        return True


    ##########################################################  
    def rollback_file(self, rollback_file, rollback_data, cont=False):
        rollback_data = str(rollback_data).strip()
        if not rollback_data:
            return True

        print(" "*6 + "Rollback " + rollback_file)

        if cont:
            when_error = "continue"
            when_exists = "skip"
        else:
            when_error = "break"
            when_exists = "error"
        self.error_text = ""

        # Check if this is a new format file (YAML-style with environment sections)
        yaml_style = self.is_yaml_style_format(rollback_data)

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
                                        (self.env and section_name.lower() == self.env.lower()) or 
                                        (self.env and self.env.lower() in [env.strip().lower() for env in section_name.split(',')]))
                        
                        if should_execute and section_content and str(section_content).strip():
                            print(" "*6 + f"Rolling back section: {section_name}")
                            if not self.rollback_data_section(str(section_content).strip(), when_exists, when_error):
                                return False
                else:
                    self.error_text = "Invalid YAML format in rollback: expected dictionary structure"
                    print(" "*6 + "!!! " + self.error_text)
                    return False
                    
            except yaml.YAMLError as e:
                self.error_text = f"Error parsing YAML rollback data: {e}"
                print(" "*6 + "!!! " + self.error_text)
                return False

        else:
            # Use original logic for non-YAML format
            if not self.rollback_data_section(rollback_data, when_exists, when_error):
                return False

        self._finish_migration(status=self.STATUS_ROLLBACK)
        print(" "*6 + "Finished")
        return True

    ##########################################################  
    def rollback_data_section(self, rollback_data, when_exists="error", when_error="break"):
        """Helper method to rollback a single YAML section"""
        self.error_text = ""
        for sql in split_string(rollback_data):
            sql = sql.strip()
            if sql:
                object_type, object_schema, object_name, description, self.error_text = self.db.migrate(sql, when_exists, env=self.env)
                self._save_log(release_id=self.release_id, migration_id=self.migration_id, 
                               object_type=object_type, object_schema=object_schema, object_name=object_name, 
                                action_info=description, error_text=self.error_text)
                if self.error_text and (when_error == "break"):
                    print(" "*6 + "!!! Process terminated")
                    self._finish_migration(status=self.STATUS_ERROR, error_text=self.error_text)
                    return False
        return True


    ##########################################################  
    def create_new_release(self):

        new_release_code = "000001"
        if self.releases:
            last_release = self.get_last_release()
            new_release_code = last_release["code"]
            new_release_int = int(new_release_code) + 1
            new_release_code = str(new_release_int).rjust(6, "0")

        new_release = {"code":new_release_code, "name":"", "comment":""}
        self.releases.append(new_release)


        # Create catalog
        set_releases_dir = self._get_actual_release_path(self.set_dir)
        release_dir = "/".join([set_releases_dir, new_release_code])
        try: 
            os.mkdir(release_dir) 
        except OSError as error: 
            print("!!! Error creating catalog", release_dir, error)
            return False
        migration_file = "/".join([release_dir, "migration.json"])
        empty_migrtion = [{"migration" : "create.sql", "rollback" : "rollback.sql"}]
        if not write_json(migration_file, empty_migrtion):
            print("!!! Error writing to file " + migration_file)
            return False


        if not write_json(self.release_filename, self.releases):
            print("!!! Error writing to file " + release_filename)
            return False

        return True



    ##########################################################  
    def check_core_version(self):
        current_core_release = self.get_last_release(is_core=True)
        self.error_text, core_db_version = self.get_db_version(is_core=True)
        if self.error_text:
            print("!!!", self.error_text)
            return False

        if core_db_version != current_core_release["code"]:
            self.error_text = f"Missmaching core version. Need: {current_core_release['code']}, real: {core_db_version}"
            print("!!!", self.error_text)
            return False
        return True

    ##########################################################  
    def get_db_version(self, is_core=False):

        code = self.CORE_VERSION if is_core else self.MIGRATION_VERSION
        if self.db.db_type == "ORCL":
            sql  = "SELECT value FROM hdset_params WHERE code = :code"
        else:
            sql  = "SELECT value FROM hdset_params WHERE code = %s"
        error_text, row = self.db.get_first_row(sql, [code,])
        if error_text:
            return error_text, None
        return None, row[0]


    ##########################################################  
    def read_migration(self, release_code):
        set_dir = self.core_dir if self.is_core else self.set_dir
        set_releases_dir = self._get_actual_release_path(set_dir)
        self.release_dir = "/".join([set_releases_dir, release_code])
        self.migration_filename = "/".join([self.release_dir, "migration.json"])
        
        error_text, migrations = read_json(self.migration_filename)
        return error_text, migrations

    ##########################################################  
    def _get_actual_release_path(self, base_set_dir):
        """Determines the correct base path for releases (either set_dir or set_dir/releases)."""
        releases_subdir = os.path.join(base_set_dir, "releases")
        if os.path.isdir(releases_subdir):
            return releases_subdir
        return base_set_dir


    ##########################################################  
    def _sql_logging(self, object_type, object_schema, object_name, description, error_text0):
        if self.is_core:
            return ""

        success_cnt = 0 if error_text0 else 1
        error_cnt = 1 if error_text0 else 0

        if self.db.db_type == "ORCL":
            update_release_sql = """UPDATE hdset_releases
                                      SET success_cnt = success_cnt + :success_cnt,
                                          error_cnt = error_cnt + :error_cnt
                                      WHERE release_id = :release_id"""
            update_migration_sql = """UPDATE hdset_migrations
                                        SET success_cnt = success_cnt + :success_cnt,
                                            error_cnt = error_cnt + :error_cnt
                                        WHERE migration_id = :migration_id"""
        else:
            update_release_sql = """UPDATE hdset_releases
                                      SET success_cnt = success_cnt + %s,
                                          error_cnt = error_cnt + %s
                                      WHERE release_id = %s"""
            update_migration_sql = """UPDATE hdset_migrations
                                        SET success_cnt = success_cnt + %s,
                                            error_cnt = error_cnt + %s
                                        WHERE migration_id = %s"""


        error_text = self.db.execute(update_release_sql, [success_cnt, error_cnt, self.release_id, ])
        if error_text:
            print("Update release error:", error_text)
            self.db.rollback()
            return error_text
        error_text = self.db.execute(update_migration_sql, [success_cnt, error_cnt, self.migration_id, ])
        if error_text:
            print("Update migration error:", error_text)
            self.db.rollback()
            return error_text
        self._save_log(release_id=self.release_id, migration_id=self.migration_id, 
                       object_type=object_type, object_schema=object_schema, object_name=object_name, 
                        action_info=description, error_text=error_text0)
        return ""


    ##########################################################  
    def _insert_migration(self, migration, migration_data, rollback_data):
        if self.is_core:
            return ""

        self.migration_id = self.db.get_id()
        if self.db.db_type == "ORCL":
            insert_sql = """INSERT INTO hdset_migrations(migration_id,
                                                       release_id, migration_file, rollback_file,
                                                       success_cnt, error_cnt, status, i_time,
                                                       migration_data, rollback_data)
                              VALUES(:migration_id,
                                     :release_id, :migration_file, :rollback_file,
                                     0, 0, :status, SYSDATE,
                                     :migration_data, :rollback_data)"""
        else:
            insert_sql = """INSERT INTO hdset_migrations(migration_id,
                                                       release_id, migration_file, rollback_file,
                                                       success_cnt, error_cnt, status, i_time,
                                                       migration_data, rollback_data)
                              VALUES(%s,
                                     %s, %s, %s,
                                     0, 0, %s, CURRENT_TIMESTAMP,
                                     %s, %s)"""

        error_text = self.db.execute(insert_sql, 
                                     [self.migration_id,
                                      self.release_id, migration["migration"], migration["rollback"],
                                      self.STATUS_INSERT,
                                      migration_data, rollback_data, ])
        if error_text:
            print("Insert migration error:", error_text)
            self.db.rollback()
            return error_text

        if self.db.db_type == "ORCL":
            update_sql = """UPDATE hdset_releases
                              SET file_cnt = file_cnt + 1
                              WHERE release_id = :release_id"""
        else:
            update_sql = """UPDATE hdset_releases
                              SET file_cnt = file_cnt + 1
                              WHERE release_id = %s"""

        err_text = self.db.execute(update_sql, [self.release_id, ])
        if err_text:
            print("Update file_cnt error:", error_text)
            self.db.rollback()
            return err_text

        self.db.commit()
        self._save_log(release_id=self.release_id, migration_id=self.migration_id, action_info=f"Start migration")
        return ""

    ##########################################################  
    def _finish_migration(self, status=None, error_text=""):
        if self.is_core:
            return ""

        if self.action_type == self.ACTION_MIGRATE:
            action_info = "Migration finished"
        elif self.action_type == self.ACTION_ROLLBACK:
            action_info = "Rollback of file finished"
        else:
            action_info = "Action finished"

        if self.db.db_type == "ORCL":
            update_sql = """UPDATE hdset_migrations
                              SET status = :status,
                                  error_text = :error_text
                              WHERE migration_id = :migration_id"""
        else:
            update_sql = """UPDATE hdset_migrations
                              SET status = %s,
                                  error_text = %s
                              WHERE migration_id = %s"""
        err_text = self.db.execute(update_sql, [status, error_text, self.migration_id, ])
        if err_text:
            print("Update migration status error:", err_text)
            self.db.rollback()
            return err_text
        self.db.commit()
        self._save_log(release_id=self.release_id, migration_id=self.migration_id, action_info=action_info, error_text=error_text)
        return ""


    ##########################################################  
    def _insert_release(self, release):
        if self.is_core:
            return ""


        self.release_id = self.db.get_id()
        if self.db.db_type == "ORCL":
            insert_sql = """INSERT INTO hdset_releases(release_id, 
                                                        release_code, release_name, release_comment,
                                                        file_cnt, success_cnt, error_cnt,
                                                        status, i_time, i_user)
                              VALUES(:id,
                                     :code, :name, :com,
                                     0, 0, 0,
                                     'I', SYSDATE, USER)"""
        else:
            insert_sql = """INSERT INTO hdset_releases(release_id, 
                                                        release_code, release_name, release_comment,
                                                        file_cnt, success_cnt, error_cnt,
                                                        status, i_time, i_user)
                              VALUES(%s,
                                     %s, %s, %s,
                                     0, 0, 0,
                                     'I', CURRENT_TIMESTAMP, SESSION_USER)"""
        error_text = self.db.execute(insert_sql, [self.release_id, release["code"], release["name"], release["comment"], ])
        if error_text:
            self.db.rollback()
            return error_text
        self.db.commit()
        self._save_log(release_id=self.release_id, action_info=f"Create new release")
        return ""


    ##########################################################  
    def _finish_release(self, status=None, error_text=""):
        if self.is_core:
            return ""

        if self.action_type == self.ACTION_MIGRATE:
            action_info = "Release finished"
        elif self.action_type == self.ACTION_ROLLBACK:
            action_info = "Rollback of release finished"
        else:
            action_info = "Action finished"

        # print(status, error_text, self.release_id)
        if self.db.db_type == "ORCL":
            update_sql = """UPDATE hdset_releases
                              SET status = :status,
                                  err_text = :error_text
                              WHERE release_id = :release_id"""
        else:
            update_sql = """UPDATE hdset_releases
                              SET status = %s,
                                  err_text = %s
                              WHERE release_id = %s"""

        err_text = self.db.execute(update_sql, [status, error_text, self.release_id, ])
        if err_text:
            self.db.rollback()
            return err_text
        self.db.commit()
        self._save_log(release_id=self.release_id, action_info=action_info, error_text=error_text)
        return ""


    ##########################################################  
    def _delete_release(self, release_id):
        if self.is_core:
            return ""

        if self.db.db_type == "ORCL":
            delete_sql = """UPDATE hdset_releases
                               SET status = 'D'
                               WHERE release_id = :release_id
                         """
        else:
            delete_sql = """UPDATE hdset_releases
                               SET status = 'D'
                               WHERE release_id = %s
                         """

        error_text = self.db.execute(delete_sql, [release_id, ])
        if error_text:
            self.db.rollback()
            return error_text
        self.db.commit()
        self._save_log(release_id=release_id, action_info=f"Delete release when creating new one")
        return ""


    ##########################################################  
    def _save_log(self, release_id=None, migration_id=None, 
                        object_type=None, object_schema=None, object_name=None, 
                        action_info=None, error_text=None):
        if self.is_core:
            return ""
        status = self.STATUS_ERROR if error_text else self.STATUS_SUCCESS
        if object_name and not object_schema:
            object_schema = self.db.current_schema

        id = self.db.get_id()
        if self.db.db_type == "ORCL":
            insert_sql = """INSERT INTO hdset_logs(log_id,
                                                   release_id, migration_id, i_time, i_user, status,
                                                   action_type, object_type, object_schema, object_name,
                                                   action_info, error_text)
                              VALUES(:id,
                                     :release_id, :migration_id, SYSDATE, USER, :status,
                                     :action_type, :object_type, :object_schema, :object_name,
                                     :action_info, :error_text)"""
        else:
            insert_sql = """INSERT INTO hdset_logs(log_id,
                                                   release_id, migration_id, i_time, i_user, status,
                                                   action_type, object_type, object_schema, object_name,
                                                   action_info, error_text)
                              VALUES(%s,
                                     %s, %s, CURRENT_TIMESTAMP, SESSION_USER, %s,
                                     %s, %s, %s, %s,
                                     %s, %s)"""

        error_text = self.db.execute(insert_sql, 
                                          [id,
                                           release_id, migration_id, status,
                                           self.action_type, object_type, object_schema, object_name,
                                           action_info, error_text, ])
        if error_text:
            print("Logging error:", err_text)
            self.db.rollback()
            return error_text
        self.db.commit()
        return ""
