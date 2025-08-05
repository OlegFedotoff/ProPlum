
from core.handler import Handler

def migrate(db, set, env, is_core=False, start_release="", cont=False):

    if not set:
        print("\n!!! Missing parameter <repository>")
        return False

    print("\nENVIRONMENT:", env)
    print("Migration repository <%s>" % (set,))
    handler = Handler(db, set, env)
    if not handler.init():
        return False

    print("Migration in schema <%s>\n" % (handler.schema,))

    if not is_core and not handler.check_core_version():
        return False

    if not handler.migrate(is_core=is_core, start_release=start_release, cont=cont):
        return False

    print("\nDone")
    return True
