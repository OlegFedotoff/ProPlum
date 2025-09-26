import re
import datetime
import cx_Oracle
import psycopg2


def create_table_sql(sql_template):
    # Поиск параметров partition с учетом пробелов
    pattern = r"\{\{\s*PARTITIONS\s*,\s*START\s*=\s*(\S+)\s*,\s*END\s*=\s*(\S+)\s*,\s*INTERVAL\s*=\s*(\S+)\s*\}\}"
    match = re.search(pattern, sql_template, re.IGNORECASE)
    if match:
        start_date, end_date, interval = match.groups()
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        partitions_sql = ""

        if interval == 'month':
            current = start
            while current < end:
                next_month = (current.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
                if next_month > end:
                    next_month = end + datetime.timedelta(days=1)
                partition_name = current.strftime("m_%m_%Y")
                partitions_sql += f"PARTITION {partition_name} START ('{current.date()}') INCLUSIVE END ('{next_month.date()}') EXCLUSIVE,\n"
                current = next_month
        elif interval == 'day':
            current = start
            while current < end:
                next_day = current + datetime.timedelta(days=1)
                partition_name = current.strftime("d_%d_%m_%Y")
                partitions_sql += f"PARTITION {partition_name} START ('{current.date()}') INCLUSIVE END ('{next_day.date()}') EXCLUSIVE,\n"
                current = next_day

        # Удаление последней запятой
        partitions_sql = partitions_sql.rstrip(",\n")
        # Замена маркера на SQL с partition
        final_sql = re.sub(pattern, f" (\n{partitions_sql}\n)", sql_template, flags=re.IGNORECASE)
    else:
        # Если маркер partitions не найден, используем исходный шаблон
        final_sql = sql_template

    return final_sql


########################################################################
class Database(object):

    def __init__(self, connection, schema, password, host, port, db_type, db_dev=None, db_test=None, db_prod=None, host_dev=None):
        super(Database, self).__init__()
        self.connection = connection
        self.schema = schema
        schema = schema.strip()
        if db_type == "ORCL":
            schema = schema.upper()
        self.current_schema = schema
        self.password = password
        self.host = host
        self.host_dev = host_dev
        self.port = port
        self.db_type = db_type
        self.db_dev = db_dev
        self.db_test = db_test
        self.db_prod = db_prod

    def get_new_db(self, db_name):
        new_db = None
        if self.db_type != "ORCL":
            host_name = self.host_dev
            if db_name == self.db_prod:
                host_name = self.host
            try:
                connection = psycopg2.connect(dbname = db_name, 
                                                  user = self.schema, 
                                                  password = self.password, 
                                                  host = host_name,
                                                  port = self.port)
            except psycopg2.Error as e:
                print("!!! Error connecting to GP %s(%s)\n%s" % (host_name, self.schema, str(e)))
                return None
            new_db = Database(connection, self.schema, self.password, 
                              self.host, self.port, self.db_type,
                              db_dev=self.db_dev, db_test=self.db_test, db_prod=self.db_prod, host_dev=self.host_dev)
        return new_db
        
    def exists(self, sql, params):
        cur = self.connection.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        if row:
            return row[0] is not None
        else:
            return False

    def get_id(self):
        if self.db_type == "ORCL":
            sql = "SELECT hdset_seq.NEXTVAL FROM dual"
        else:
            sql = "select nextval('hdset_seq')"
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
        return row[0]

    def get_first_row(self, sql, params=[]):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
            return None, row
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            error_text = error.message
            return error_text, None
        except psycopg2.Error as e:
            error_text = str(e)
            self.rollback()
            return error_text, None


    def get_all_rows(self, sql, params=[]):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            return None, rows
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            error_text = error.message
            return error_text, None
        except psycopg2.Error as e:
            error_text = str(e)
            self.rollback()
            return error_text, None

    def set_current_schema(self, schema):
        if self.db_type == "ORCL":
            self.current_schema = schema.strip().upper()
            sql = f"ALTER SESSION SET CURRENT_SCHEMA = {schema}"
            commit = False
        else:
            self.current_schema = schema.strip()
            sql = f'SET search_path TO "{schema}"'
            commit = True
        return self.execute(sql, commit=commit)

    def check_object_exists(self, object_type, object_schema, object_name):

        if self.db_type == "ORCL":
            if object_schema:
                object_schema = object_schema.strip().upper()
            else:
                object_schema = self.current_schema
        else:
            if object_schema:
                object_schema = object_schema.strip()
            else:
                object_schema = self.current_schema


        sql = ""

        if self.db_type == "ORCL":
            if object_type == "TABLE":
                sql = "SELECT 1 FROM all_tables WHERE owner = :object_schema AND table_name = :object_name"
            elif object_type == "INDEX":
                sql = "SELECT 1 FROM all_indexes WHERE owner = :object_schema AND index_name = :object_name"
            elif object_type == "SEQUENCE":
                sql = "SELECT 1 FROM all_sequences WHERE sequence_owner = :object_schema AND sequence_name = :object_name"
        else:
            if object_type == "TABLE":
                sql = 'SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s'
            elif object_type == "INDEX":
                sql = 'SELECT 1 FROM pg_indexes WHERE schemaname = %s AND indexname = %s'
            elif object_type == "SEQUENCE":
                sql = f"""SELECT to_regclass('"{object_schema}".{object_name}')"""


        if sql:
            is_exists = self.exists(sql, (object_schema, object_name))
        else:
            is_exists = False
        return is_exists


    def migrate(self, sql, when_exists="error", set=None, env="dev"):
        result = ""
        skip = False
        object_type, object_schema, object_name, description = get_sql_info(sql)
        if description == "Empty sql":  
            return object_type, object_schema, object_name, description, ""

        if self.db_type == "ORCL":
            object_schema = object_schema.upper()
            object_name = object_name.upper()
        else:
            object_schema = object_schema.lower()
            object_name = object_name.lower()
        print(" "*8, "..", description)

        main_action = description[10:].upper()
        if main_action.startswith("CREATE") and object_type in["TABLE", "INDEX", "SEQUENCE", ]:
            if self.check_object_exists(object_type, object_schema, object_name):
                info = " ".join([object_type, object_name, "already exists"])
                if when_exists == "error":
                    print(" "*8, "!!!", info)
                    return object_type, object_schema, object_name, description, info
                else:
                    print(" "*8, info, "Skip creating")
                    skip = True

        role_ro = f"role_{set}_ro"
        role_rw = f"role_{set}_rw"

        if not skip:

            # replace external_connect for external/foreign table 
            # connect to opposite db
            if object_type == "TABLE":
                if env.lower() == "prod":
                    external_connect = "adwhdev.komus.net:5432/ADWHDEV&USER=ml_reader&PASS=Welcome_1212"
                else:
                    external_connect = "adwh.komus.net:5432/ADWH&USER=ml_reader&PASS=Welcome_1212"
                sql = sql.replace("{{external_connect}}", external_connect)
                sql = create_table_sql(sql)

            # Replace template variables for schema and owner
            if set:
                target_schema = set
                owner = f"role_{set}_owner"
                sql = sql.replace("${target_schema}", target_schema)
                sql = sql.replace("${owner}", owner)

            sql = sql.replace("{{roles}}", role_ro+","+role_rw)
            sql = sql.replace("%", "%%")


            result = self.execute(sql)
            if result:
                print(" "*8, "!!!", result)

        if not result and main_action.startswith("CREATE") and set:

            if object_type == "TABLE":
                # GRANT SELECT
                if self.db_type == "ORCL":
                    if object_schema:
                        grant_sql = f'GRANT SELECT ON {object_schema}.{object_name} TO {role_ro}'
                    else:
                        grant_sql = f"GRANT SELECT ON {object_name} TO {role_ro}"
                else:
                    if object_schema:
                        grant_sql = f'GRANT SELECT ON "{object_schema}".{object_name} TO {role_ro}'
                    else:
                        grant_sql = f"GRANT SELECT ON {object_name} TO {role_ro}"
                error_text = self.execute(grant_sql)
                if error_text:
                    print(" "*8, "!!!", grant_sql, error_text)
                    return object_type, object_schema, object_name, description, error_text
                # GRANT INSERT UPDATE DELETE
                if self.db_type == "ORCL":
                    if object_schema:
                        grant_sql = f"GRANT INSERT,UPDATE,DELETE ON {object_schema}.{object_name} TO {role_rw}"
                    else:
                        grant_sql = f"GRANT INSERT,UPDATE,DELETE ON {object_name} TO {role_rw}"
                else:
                    if object_schema:
                        grant_sql = f'GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON "{object_schema}".{object_name} TO {role_rw}'
                    else:
                        grant_sql = f"GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON {object_name} TO {role_rw}"
                error_text = self.execute(grant_sql)
                if error_text:
                    print(" "*8, "!!!", grant_sql, error_text)
                    return object_type, object_schema, object_name, description, error_text

            if object_type == "VIEW":
                # GRANT SELECT
                if self.db_type == "ORCL":
                    if object_schema:
                        grant_sql = f'GRANT SELECT ON {object_schema}.{object_name} TO {role_ro}'
                    else:
                        grant_sql = f"GRANT SELECT ON {object_name} TO {role_ro}"
                else:
                    if object_schema:
                        grant_sql = f'GRANT SELECT ON "{object_schema}".{object_name} TO {role_ro}, {role_rw}'
                    else:
                        grant_sql = f"GRANT SELECT ON {object_name} TO {role_ro}, {role_rw}"
                error_text = self.execute(grant_sql)
                if error_text:
                    print(" "*8, "!!!", grant_sql, error_text)
                    return object_type, object_schema, object_name, description, error_text

            if object_type == "SEQUENCE":
                # GRANT SEQUENCE
                if self.db_type == "ORCL":
                    if object_schema:
                        grant_sql = f"GRANT SELECT ON {object_schema}.{object_name} TO {role_rw}"
                    else:
                        grant_sql = f"GRANT SELECT ON {object_name} TO {role_rw}"
                else:
                    if object_schema:
                        grant_sql = f'GRANT ALL ON "{object_schema}".{object_name} TO {role_rw}'
                    else:
                        grant_sql = f"GRANT ALL ON {object_name} TO {role_rw}"
                print(grant_sql)
                error_text = self.execute(grant_sql)
                print(error_text)
                if error_text:
                    print(" "*8, "!!!", grant_sql, error_text)
                    return object_type, object_schema, object_name, description, error_text

        return object_type, object_schema, object_name, description, result

    def execute(self, sql, params=[], commit=False):
        result = ""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                if commit:
                    self.commit()
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            result = error.message
        except psycopg2.Error as e:
            result = str(e)
            self.rollback()
        return result

    def commit(self):
        self.connection.commit()
    def rollback(self):
        self.connection.rollback()





########################################################################
def get_sql_info(sql):
    def get_sql_description(sql_type, object_type, object_schema=None, object_name=None):
        if object_schema and object_name:
            sql_description = 'Executing %s %s %s.%s' % (sql_type, object_type, object_schema, object_name)
        elif object_name:
            sql_description = 'Executing %s %s %s' % (sql_type, object_type, object_name)
        else:
            sql_description = 'Executing %s' % object_type
        return sql_description

    #remove all comments
    sql = remove_comments(sql)

    if not sql.strip():
        #empty sql
        return '', '', '', 'Empty sql'

    #create or replace
    create_or_replace_pattern = re.compile(r"^[\s]*CREATE[\s]+OR[\s]+REPLACE[\s]+([A-Z]+)[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_or_replace_package_body_pattern = re.compile(r"^[\s]*CREATE[\s]+OR[\s]+REPLACE[\s]+PACKAGE[\s]+BODY[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_or_replace_type_body_pattern = re.compile(r"^[\s]*CREATE[\s]+OR[\s]+REPLACE[\s]+TYPE[\s]+BODY[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    search_obj = create_or_replace_pattern.search(sql)
    if search_obj:
        object_type = search_obj.group(1).upper()
        object_name = search_obj.group(2).upper()
        object_type_upper = object_type.upper()
        object_name_upper = object_name.upper()
        if object_type_upper == 'PACKAGE' and object_name_upper == 'BODY':
            search_obj2 = create_or_replace_package_body_pattern.search(sql)
            if search_obj2:
                object_type = 'PACKAGE BODY'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'TYPE' and object_name_upper == 'BODY':
            search_obj2 = create_or_replace_type_body_pattern.search(sql)
            if search_obj2:
                object_type = 'TYPE BODY'
                object_name = search_obj2.group(1)
        names = object_name.split('.')
        if len(names) == 1:
            object_schema = ''
        else:
            object_schema = names[0].strip()
            object_name = names[1].strip()
        return object_type, object_schema, object_name, get_sql_description('CREATE OR REPLACE', object_type, object_schema, object_name)

    #create
    create_pattern = re.compile(r"^[\s]*CREATE[\s]+([A-Z]+)[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_package_body_pattern = re.compile(r"^[\s]*CREATE[\s]+PACKAGE[\s]+BODY[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_type_body_pattern = re.compile(r"^[\s]*CREATE[\s]+TYPE[\s]+BODY[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_materialized_view_pattern = re.compile(r"^[\s]*CREATE[\s]+MATERIALIZED[\s]+VIEW[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_external_table_pattern = re.compile(r"^[\s]*CREATE[\s]+EXTERNAL[\s]+TABLE[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_readable_external_table_pattern = re.compile(r"^[\s]*CREATE[\s]+READABLE[\s]+EXTERNAL[\s]+TABLE[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_writable_external_table_pattern = re.compile(r"^[\s]*CREATE[\s]+WRITABLE[\s]+EXTERNAL[\s]+TABLE[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    create_foreign_table_pattern = re.compile(r"^[\s]*CREATE[\s]+FOREIGN[\s]+TABLE[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    search_obj = create_pattern.search(sql)
    if search_obj:
        object_type = search_obj.group(1).upper()
        object_name = search_obj.group(2)
        object_type_upper = object_type.upper()
        object_name_upper = object_name.upper()
        if object_type_upper == 'PACKAGE' and object_name_upper == 'BODY':
            search_obj2 = create_package_body_pattern.search(sql)
            if search_obj2:
                object_type = 'PACKAGE BODY'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'TYPE' and object_name_upper == 'BODY':
            search_obj2 = create_type_body_pattern.search(sql)
            if search_obj2:
                object_type = 'TYPE BODY'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'MATERIALIZED' and object_name_upper == 'VIEW':
            search_obj2 = create_materialized_view_pattern.search(sql)
            if search_obj2:
                object_type = 'MATERIALIZED VIEW'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'EXTERNAL' and object_name_upper == 'TABLE':
            search_obj2 = create_external_table_pattern.search(sql)
            if search_obj2:
                object_type = 'TABLE'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'READABLE' and object_name_upper == 'EXTERNAL':
            search_obj2 = create_readable_external_table_pattern.search(sql)
            if search_obj2:
                object_type = 'TABLE'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'WRITABLE' and object_name_upper == 'EXTERNAL':
            search_obj2 = create_writable_external_table_pattern.search(sql)
            if search_obj2:
                object_type = 'TABLE'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'FOREIGN' and object_name_upper == 'TABLE':
            search_obj2 = create_foreign_table_pattern.search(sql)
            if search_obj2:
                object_type = 'TABLE'
                object_name = search_obj2.group(1)
        names = object_name.split('.')
        if len(names) == 1:
            object_schema = ''
        else:
            object_schema = names[0].strip()
            object_name = names[1].strip()
        return object_type, object_schema, object_name, get_sql_description('CREATE', object_type, object_schema, object_name)

    #alter
    alter_pattern = re.compile(r"^[\s]*ALTER[\s]+([A-Z]+)[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    alter_materialized_view_pattern = re.compile(r"^[\s]*ALTER[\s]+MATERIALIZED[\s]+VIEW[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    alter_external_table_pattern = re.compile(r"^[\s]*ALTER[\s]+EXTERNAL[\s]+TABLE[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    alter_foreign_table_pattern = re.compile(r"^[\s]*ALTER[\s]+FOREIGN[\s]+TABLE[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    search_obj = alter_pattern.search(sql)
    if search_obj:
        object_type = search_obj.group(1)
        object_name = search_obj.group(2)
        object_type_upper = object_type.upper()
        object_name_upper = object_name.upper()
        if object_type_upper == 'MATERIALIZED' and object_name_upper == 'VIEW':
            search_obj2 = alter_materialized_view_pattern.search(sql)
            if search_obj2:
                object_type = 'MATERIALIZED VIEW'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'EXTERNAL' and object_name_upper == 'TABLE':
            search_obj2 = alter_external_table_pattern.search(sql)
            if search_obj2:
                object_type = 'TABLE'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'FOREIGN' and object_name_upper == 'TABLE':
            search_obj2 = alter_foreign_table_pattern.search(sql)
            if search_obj2:
                object_type = 'TABLE'
                object_name = search_obj2.group(1)
        names = object_name.split('.')
        if len(names) == 1:
            object_schema = ''
        else:
            object_schema = names[0].strip()
            object_name = names[1].strip()
        return object_type, object_schema, object_name, get_sql_description('ALTER', object_type, object_schema, object_name)

    #drop
    drop_pattern = re.compile(r"^[\s]*DROP[\s]+[\s]*([A-Z]+)[\s]+[\s]*([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    drop_materialized_view_pattern = re.compile(r"^[\s]*DROP[\s]+MATERIALIZED[\s]+VIEW[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    drop_external_table_pattern = re.compile(r"^[\s]*DROP[\s]+EXTERNAL[\s]+TABLE[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    drop_foreign_table_pattern = re.compile(r"^[\s]*DROP[\s]+FOREIGN[\s]+TABLE[\s]+([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    drop_if_exists_pattern = re.compile(r"^[\s]*DROP[\s]+[\s]*([A-Z]+)[\s]+[\s]*IF[\s]+EXISTS[\s]+[\s]*([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    search_obj = drop_pattern.search(sql)
    if search_obj:
        object_type = search_obj.group(1)
        object_name = search_obj.group(2)
        object_type_upper = object_type.upper()
        object_name_upper = object_name.upper()
        if object_type_upper == 'MATERIALIZED' and object_name_upper == 'VIEW':
            search_obj2 = drop_materialized_view_pattern.search(sql)
            if search_obj2:
                object_type = 'MATERIALIZED VIEW'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'EXTERNAL' and object_name_upper == 'TABLE':
            search_obj2 = drop_external_table_pattern.search(sql)
            if search_obj2:
                object_type = 'TABLE'
                object_name = search_obj2.group(1)
        elif object_type_upper == 'FOREIGN' and object_name_upper == 'TABLE':
            search_obj2 = drop_foreign_table_pattern.search(sql)
            if search_obj2:
                object_type = 'TABLE'
                object_name = search_obj2.group(1)
        elif object_name_upper == 'IF':
            search_obj3 = drop_if_exists_pattern.search(sql)
            if search_obj3:
                object_type = search_obj3.group(1)
                object_name = search_obj3.group(2)
        names = object_name.split('.')
        if len(names) == 1:
            object_schema = ''
        else:
            object_schema = names[0].strip()
            object_name = names[1].strip()
        return object_type, object_schema, object_name, get_sql_description('DROP', object_type, object_schema, object_name)

    #other
    other_pattern = re.compile(r"^[\s]*([a-zA-Z0-9_$.]+)", re.IGNORECASE)
    search_obj = other_pattern.search(sql)
    if search_obj:
        object_type = search_obj.group(1)
        return object_type, '', '', get_sql_description('', object_type)
    else:
        object_type = 'Unknown sql'
        return object_type, '', '', get_sql_description('', object_type)


########################################################################
def remove_comments(source):
    source = re.sub(re.compile("/\*.*?\*/",re.DOTALL) ,"" ,source) # remove all occurrences streamed comments (/*COMMENT */) from source
    source = re.sub(re.compile("--.*?$" ,re.DOTALL+re.MULTILINE) ,"" ,source) # remove all occurrence single-line comments (--COMMENT\n ) from source
    return source
