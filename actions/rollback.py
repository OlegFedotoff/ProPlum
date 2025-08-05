
from core.handler import Handler

def rollback(db, set, env, release_code="", cont=False):

    if not set:
        print("\n!!! Missing parameter <repository>")
        return False

    if not release_code:
        print("\n!!! Missing parameter <release-code>")
        return False

    print("\nENVIRONMENT:", env)
    print("Rollback repository <%s>, release: %s" % (set, release_code,))
    handler = Handler(db, set, env)
    if not handler.init():
        return False

    print("Rollback in schema <%s>\n" % (handler.schema,))

    if not handler.check_core_version():
        return False

    if not handler.rollback(release_code=release_code, cont=cont):
        return False

    print("\nDone")
    return True
