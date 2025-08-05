from core.handler import Handler

def release_list(db, set, env, level=0):

    if not set:
        print("\n!!! Missing parameter <repository>")
        return False

    print("\nRelease list")
    print(f"Repository {set}({env})")
    handler = Handler(db, set=set, env=env)
    if not handler.init():
        return False

    mask = "{:6} {:6} {:20} {:5} {:7} {:6} {:6} {:19} {:10} {:20}"
    mask_logs = "       ..{:40} {:15} {:1} {:10} {:15} {:20}"
    print("\n")
    print(mask.format("ID", "Code", "Name", "Files", "Success", "Errors", "Status", "Time", "User", "Text"))
    print("-"*100)

    filt = "WHERE status = 'S'" if level == 0 else ""

    sql = f"""
            SELECT release_id, release_code, COALESCE(release_name, '[EMPTY]'),
                   file_cnt, success_cnt, error_cnt,
                   status, TO_CHAR(i_time, 'DD.MM.YYYY HH24:MI:SS') AS i_time,
                   i_user, COALESCE(err_text, '[EMPTY]')
              FROM hdset_releases
              {filt}
              ORDER BY release_id
          """
          
    if db.db_type == "ORCL":
        select_logs = """
                      SELECT COALESCE(l.action_info, '[EMPTY]'), 
                             CASE WHEN l.action_type = 'M' THEN COALESCE(m.migration_file, '[EMPTY]')
                                  WHEN l.action_type = 'R' THEN COALESCE(m.rollback_file, '[EMPTY]')
                                  ELSE '[EMPTY]'
                             END AS file_name, 
                             l.action_type, 
                             COALESCE(l.object_type, '[EMPTY]'), COALESCE(l.object_name, '[EMPTY]'), 
                             COALESCE(l.error_text, '[EMPTY]')
                        FROM hdset_logs l, hdset_migrations m 
                        WHERE l.migration_id = m.migration_id (+)
                          AND l.release_id = :release_id
                        ORDER BY log_id
                      """
    else:
        select_logs = """
                      SELECT COALESCE(l.action_info, '[EMPTY]'), 
                             CASE WHEN l.action_type = 'M' THEN COALESCE(m.migration_file, '[EMPTY]')
                                  WHEN l.action_type = 'R' THEN COALESCE(m.rollback_file, '[EMPTY]')
                                  ELSE '[EMPTY]'
                             END AS file_name, 
                             l.action_type, 
                             COALESCE(l.object_type, '[EMPTY]'), COALESCE(l.object_name, '[EMPTY]'), 
                             COALESCE(l.error_text, '[EMPTY]')
                        FROM hdset_logs l LEFT JOIN hdset_migrations m ON (l.migration_id = m.migration_id) 
                        WHERE l.release_id = %s
                        ORDER BY log_id
                      """


    # db.set_current_schema(set)
    error_text, releases = db.get_all_rows(sql)
    max_release = "000000"
    if error_text:
        print("!!!", error_text)
        return False
    for r in releases:
        print(mask.format(*r))
        release_code = r[1]
        if release_code > max_release:
            max_release = release_code
        if level >= 2:
            error_text, logs = db.get_all_rows(select_logs, [r[0], ])
            if error_text:
                print("!!! Error get log info", error_text, r[0])
                return False
            for log in logs:
                print(mask_logs.format(*log))

    # not installed releases
    error_text, start_release = handler.get_db_version(False)
    if error_text:
        print("!!!", error_text)
        return False
    releases = [r for r in handler.releases if r["code"] > start_release]
    releases = sorted(releases, key=lambda x: x['code'])
    for r in releases:
        print(mask.format("***", r["code"], r["name"], 0, 0, 0, "", "", "", ""))

    print("\nDone")
    return True
