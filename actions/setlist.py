from core.handler import Handler

def set_list(db):

    print("\nRepository list")
    handler = Handler(db, set=None)
    if not handler.init():
        return False

    mask = "{:20} {:30} {:20}"
    print("\n")
    print(mask.format("Code", "Name", "Quota"))
    print("-"*80)

    repositories = [(k,v) for k,v in handler.sets.items()]
    repositories = sorted(repositories, key=lambda x: x[0])

    for r in repositories:
        print(mask.format(r[0], r[1]["name"], r[1]["quota"]))

    print("\nDone")
    return True
