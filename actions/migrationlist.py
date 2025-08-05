from core.handler import Handler

def migration_list(db, set, env, level=0, release_code=None, release_id=None):

    if not set:
        print("\n!!! Missing parameter <repository>")
        return False

    if not release_code and not release_id:
        print("\n!!! Release-code or release-id must be set")
        return False

    print("\nMigration")
    print(f"Repository {set}({env})")

    handler = Handler(db, set=set, env=env)
    if not handler.init():
        return False

    # get release in db by id
    if release_id:
        if db.db_type == "ORCL":
            select_release_by_id = "SELECT * FROM hdset_releases WHERE release_id = :release_id"
        else:
            select_release_by_id = "SELECT * FROM hdset_releases WHERE release_id = %s"
        error_text, release = db.get_first_row(select_release_by_id, [release_id,])
        if error_text:
            print("!!! Error get release by id:", error_text)
            return False
        installed = True

    # get release in db by code
    if release_code:
        if db.db_type == "ORCL":
            select_release_by_code = """SELECT * 
                                          FROM hdset_releases 
                                          WHERE release_code = :release_code
                                            AND status = 'S'
                                          ORDER BY release_id DESC"""
        else:
            select_release_by_code = """SELECT * 
                                          FROM hdset_releases 
                                          WHERE release_code = %s
                                            AND status = 'S'
                                          ORDER BY release_id DESC"""
        error_text, release = db.get_first_row(select_release_by_code, [release_code,])
        if error_text:
            print("!!! Error get release by code:", error_text)
            return False
        if release:
            installed = True
        else:
            installed = False

    # get not installed release
    if not installed:
        release = [r for r in handler.releases if r["code"] == release_code]
        if not release:
            print("!!! Cant find release:", release_code)
            return False
        release = release[0]
        release_id      = ""
        release_code    = release.get("code","")
        release_name    = release.get("name","")
        release_comment = release.get("comment","")
        file_cnt        = ""
        success_cnt     = ""
        error_cnt       = ""
        status          = "I"
        i_time          = ""
        i_user          = ""
        err_text        = ""
    else:
        release_id      = release[0]
        release_code    = release[1]
        release_name    = release[2]
        release_comment = release[3]
        file_cnt        = release[4]
        success_cnt     = release[5]
        error_cnt       = release[6]
        status          = release[7]
        i_time          = release[8]
        i_user          = release[9]
        err_text        = release[10]


    print(f"\nRelease: {release_code}")
    print(f"ID: {release_id}")
    print(f"Name: {release_name}")
    print(f"Comment: {release_comment}")
    print(f"Files: {file_cnt}")
    print(f"Success: {success_cnt}")
    print(f"Errors: {error_cnt}")
    print(f"Status: {status}")
    print(f"Installed: {i_time}")
    print(f"User: {i_user}")
    print(f"Text: {err_text}")


    print("")
    if installed:

        # Release Info
        if db.db_type == "ORCL":
            select_logs0 = """
                          SELECT COALESCE(l.action_info, '[ENMPTY]'), 
                                 l.status, TO_CHAR(l.i_time, 'DD.MM.YYYY HH24:MI:SS') AS i_time,
                                 l.i_user, COALESCE(l.error_text, '[EMPTY]')
                            FROM hdset_logs l
                            WHERE l.migration_id IS NULL
                              AND l.release_id = :release_id
                            ORDER BY log_id
                          """
        else:
            select_logs0 = """
                          SELECT COALESCE(l.action_info, '[ENMPTY]'), 
                                 l.status, TO_CHAR(l.i_time, 'DD.MM.YYYY HH24:MI:SS') AS i_time,
                                 l.i_user, COALESCE(l.error_text, '[EMPTY]')
                            FROM hdset_logs l
                            WHERE l.migration_id IS NULL
                              AND l.release_id = %s
                            ORDER BY log_id
                          """
        print("Release info")
        error_text, release_info = db.get_all_rows(select_logs0, [release_id,])
        if error_text:
            print("!!!", error_text)
            return False
        mask_release_info = "{:30} {:6} {:19} {:10} {:30}"
        print(mask_release_info.format("Action", "Status", "Time", "User", "Error"))
        for i in release_info:
            print(mask_release_info.format(*i))


        # Files
        if db.db_type == "ORCL":
            select_files = """
                          SELECT migration_id,
                                 COALESCE(migration_file, '[ENMPTY]'), 
                                 COALESCE(rollback_file, '[ENMPTY]'), 
                                 success_cnt, error_cnt,
                                 status, TO_CHAR(i_time, 'DD.MM.YYYY HH24:MI:SS') AS i_time,
                                 COALESCE(error_text, '[EMPTY]')
                            FROM hdset_migrations
                            WHERE release_id = :release_id
                            ORDER BY migration_id
                          """
            select_logs = """
                          SELECT COALESCE(l.action_info, '[ENMPTY]'), 
                                 COALESCE(l.object_type, '[ENMPTY]'), COALESCE(l.object_name, '[ENMPTY]'), 
                                 COALESCE(l.error_text, '[ENMPTY]')
                            FROM hdset_logs l
                            WHERE l.migration_id = :migration_id
                            ORDER BY log_id
                          """
        else:
            select_files = """
                          SELECT migration_id,
                                 COALESCE(migration_file, '[ENMPTY]'), 
                                 COALESCE(rollback_file, '[ENMPTY]'), 
                                 success_cnt, error_cnt,
                                 status, TO_CHAR(i_time, 'DD.MM.YYYY HH24:MI:SS') AS i_time,
                                 COALESCE(error_text, '[EMPTY]')
                            FROM hdset_migrations
                            WHERE release_id = %s
                            ORDER BY migration_id
                          """
            select_logs = """
                          SELECT COALESCE(l.action_info, '[ENMPTY]'), 
                                 COALESCE(l.object_type, '[ENMPTY]'), COALESCE(l.object_name, '[ENMPTY]'), 
                                 COALESCE(l.error_text, '[ENMPTY]')
                            FROM hdset_logs l
                            WHERE l.migration_id = %s
                            ORDER BY log_id
                          """

        print("\nFiles")
        error_text, files = db.get_all_rows(select_files, [release_id,])
        if error_text:
            print("!!!", error_text)
            return False

        mask_files = "{:6} {:20} {:20} {:3} {:3} {:3} {:19} {:30}"
        mask_logs = "       ..{:40} {:10} {:20} {:30}"

        print(mask_files.format("ID", "File", "Rollback", "Suc", "Err", "St", "Time", "Error"))
        for f in files:
            print(mask_files.format(*f))

            error_text, logs = db.get_all_rows(select_logs, [f[0], ])
            if error_text:
                print("!!! Error get log info", error_text, f[0])
                return False
            for log in logs:
                print(mask_logs.format(*log))


    else:     # not instlled
        error_text, migrations = handler.read_migration(release["code"])
        if error_text:
            print("!!! " + error_text)
            return False

        # Files
        mask_files = "{:20} {:20}"

        print("Files")
        print(mask_files.format("File", "Rollback"))
        for m in migrations:
            print(mask_files.format(m["migration"], m["rollback"]))

    print("\nDone")
    return True
