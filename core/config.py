import os

import configparser
import cx_Oracle
import psycopg2

class Config:
    """Interact with configuration variables."""

    configParser = configparser.ConfigParser()
    db_user = None
    db_pass = None
    host = None
    host_test = None
    host_dev = None
    port = None
    service_name = None
    connection = None
    smart = False
    db_dev = None
    db_test = None
    db_prod = None
    git_branch_check = True # Default to True
    default_set = None  # Добавляем новый атрибут для хранения схемы по умолчанию
    sets_dir = "sets"  # Добавляем новый атрибут для хранения каталога схем

    @classmethod
    def initialize(cls, config_file, env=None):
        """Start config by reading config.ini."""
        configFilePath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", config_file)
        file_exists = os.path.exists(configFilePath)
        if not file_exists:
            print("Can't find config file " + config_file)
            return False
        cls.configParser.read(configFilePath)

        cls.db_type = cls.get_value("MAIN","db_type")
        
        # Read new git_branch_check parameter
        git_check_value = cls.get_value("MAIN", "git_branch_check")
        if git_check_value is not None and git_check_value.lower() == "false":
            cls.git_branch_check = False
        else:
            cls.git_branch_check = True # Default or any other value means True

        # Read default set name from config.ini
        cls.default_set = cls.get_value("MAIN", "set")
        
        # Read sets directory from config.ini
        cls.sets_dir = cls.get_value("MAIN", "sets_dir")
        if not cls.sets_dir:
            cls.sets_dir = "sets"  # Default value if not specified

        cls.db_dev = None
        cls.db_test = None
        cls.db_prod = None

        if cls.db_type == "GP":

            cls.db_user = cls.get_value("GP","user")
            if not cls.db_user:
                print("Can't get GP user name")
                return False

            cls.host = cls.get_value("GP","host")
            if not cls.host:
                print("Empty GP host")

            cls.host_dev = cls.get_value("GP","host_dev")
            if not cls.host_dev:
                cls.host_dev = cls.host

            cls.port = cls.get_value("GP","port")
            if not cls.port:
                cls.port = 5432

            cls.db_pass = cls.get_value("GP","password")
            if not cls.db_pass:
                cls.db_pass = input("Get password for %s:" % (cls.db_user, ))
            if not cls.db_pass:
                print("Empty GP user password")
                return False

            cls.db_dev = cls.get_value("GP","db_dev")
            if not cls.db_dev:
                print("Empty GP db_dev")
                return False

            cls.db_prod = cls.get_value("GP","db_prod")
            if not cls.db_prod:
                print("Empty GP db_prod")
                return False

            db_name = cls.db_dev
            host_name = cls.host_dev
            if env == "prod":
                db_name = cls.db_prod
                host_name = cls.host

            try:
                cls.connection = psycopg2.connect(dbname = db_name, 
                                                  user = cls.db_user, 
                                                  password = cls.db_pass, 
                                                  host = host_name,
                                                  port = cls.port)
            except psycopg2.Error as e:
                print("!!! Error connecting to GP %s(%s)\n%s" % (host_name, cls.db_user, str(e)))
                return False


        else:

            cls.db_type = "ORCL"

            cls.db_user = cls.get_value("DB","user")
            if not cls.db_user:
                print("Can't get user name")
                return False

            cls.host = cls.get_value("DB","host")
            if not cls.host:
                print("Empty database host")

            cls.host_dev = cls.get_value("DB","host_dev")
            if not cls.host_dev:
                cls.host_dev = cls.host

            cls.port = cls.get_value("DB","port")
            if not cls.port:
                cls.port = 1521

            cls.service_name = cls.get_value("DB","service_name")
            if not cls.service_name:
                print("Empty database service_name")

            cls.db_pass = cls.get_value("DB","password")
            if not cls.db_pass:
                cls.db_pass = input("Get password for %s:" % (cls.db_user, ))
            if not cls.db_pass:
                print("Empty user password")
                return False

            try:
                dsn = cx_Oracle.makedsn(cls.host, cls.port, service_name=cls.service_name)
                cls.connection = cx_Oracle.connect(user=cls.db_user, password=cls.db_pass, dsn=dsn, encoding="UTF-8")
            except cx_Oracle.Error as e:
                print("!!! Error connecting to database %s(%s)\n%s" % (cls.host, cls.db_user, str(e)))
                return False
        
        return True

    @classmethod
    def get_value(cls, section, key):
        """Get prod values from config.ini."""
        try:
            value = cls.configParser.get(section, key)
        except configparser.Error:
            value = None
        return value

    @classmethod
    def get_sections(cls):
        """Get sections from config.ini."""
        return cls.configParser.sections()
