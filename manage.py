import sys
import argparse

from core.config import Config
from core.database import Database
from core.git_utils import get_current_git_branch, validate_set_with_git_branch
import actions

def main(args):
    # Handle version command first, before any config or database initialization
    if args.action == "version":
        actions.version()
        return
    
    if not Config.initialize("config.ini", env=args.env):
        print("Quit")
        sys.exit(2)


    # Определяем set_name с учетом git-ветки
    set_name = args.set
    if not set_name:
        if Config.default_set:
            set_name = Config.default_set
            print(f"Using set name '{set_name}' from config.ini")

    if Config.git_branch_check:
        # Если set_name не задан, получаем его из текущей git-ветки
        if not set_name:
            current_branch, error = get_current_git_branch()
            if error:
                print(f"!!! Error getting git branch: {error}")
                print("!!! Please specify set name explicitly using -s parameter")
                sys.exit(2)
            set_name = current_branch
            print(f"Using git branch '{set_name}' as set name")
        else:
            # Если set_name задан, проверяем соответствие с git-веткой
            is_valid, error = validate_set_with_git_branch(set_name)
            if not is_valid:
                print(f"!!! {error}")
                sys.exit(2)
            print(f"Set name '{set_name}' matches current git branch")
    elif not set_name: # If git_branch_check is false, but -s is not provided
        print("!!! Set name not specified and git branch check is disabled. Please specify set name using -s parameter.")
        sys.exit(2)
    
    # Некоторые команды не требуют подключения к БД
    if args.action in ["movereleases", "configureencoding", "newchange"]:
        if args.action == "movereleases":
            actions.move_releases(set=set_name)
        elif args.action == "configureencoding":
            actions.configure_encoding(set=set_name)
        elif args.action == "newchange":
            actions.new_change(set=set_name)
        return
    

    db = Database(Config.connection, Config.db_user, Config.db_pass, 
                  Config.host, Config.port, Config.db_type,
                  db_dev=Config.db_dev, db_test=Config.db_test, db_prod=Config.db_prod, host_dev=Config.host_dev)
    
    action = args.action
    if action == "createset":
        actions.create_set(db=db, set=set_name)
    elif action == "newrelease":
        actions.new_release(db=db, set=set_name)
    elif action == "newcorerelease":
        actions.new_release(db=db, set="hdset")
    elif action == "migrate":
        actions.migrate(db=db, set=set_name, env=args.env, start_release=args.start, cont=args.cont)
    elif action == "migratecore":
        actions.migrate(db=db, set=set_name, env=args.env, is_core=True, start_release=args.start, cont=args.cont)
    elif action == "rollback":
        actions.rollback(db=db, set=set_name, env=args.env, release_code=args.code, cont=args.cont)
    elif action == "sets":
        actions.set_list(db=db)
    elif action == "releases":
        actions.release_list(db=db, set=set_name, env=args.env, level=args.level)
    elif action == "migrations":
        actions.migration_list(db=db, set=set_name, env=args.env, level=args.level, release_code=args.code, release_id=args.id)

    elif action == "apply":
        actions.apply(db=db, set=set_name, cont=args.cont, env=args.env)
    elif action == "rollbackchange":
        actions.rollback_change(db=db, set=set_name, cont=args.cont)
    elif action == "version":
        actions.version()
        return
    else:
        print("Unknown action " + action)
        sys.exit(2)

    print("\nFinished")

    if Config.connection:
        Config.connection.close()




if __name__== "__main__":
    parser = argparse.ArgumentParser(description='DB realese manager')
    parser.add_argument('action', type=str, help='Action', 
                        choices=["createset","newrelease","newcorerelease","migrate","migratecore","rollback","sets","releases","migrations","version","movereleases","configureencoding","newchange","apply","rollbackchange"])
    parser.add_argument('-s', metavar='repositary-name', dest="set", type=str, help='Repositary Name (optional, uses current git branch if not specified)')
    parser.add_argument('-e', metavar='dev|test|prod', dest="env", type=str, help='Enviroment', choices=["dev", "test", "prod"], default="dev")
    parser.add_argument('-start', metavar='version', type=str, help='Start release version', default="")
    parser.add_argument('-c', '--continue', dest="cont", help='Continue executing when error', action="store_true")
    parser.add_argument('-l', '--level', metavar='level', type=int, dest="level", help='Detail level', default=0, choices=[0,1,2])
    parser.add_argument('-code', metavar='release-code', type=str, help='Release code', default="")
    parser.add_argument('-id', metavar='release-id', type=int, help='Release ID', default=0)
    args = parser.parse_args()
    main(args)
