import mysql.connector as mariadb


def create_summary_table(db_file):
    """ create the initial database table summary
    :param db_file: database file
    :return: Nothing
    """
    sql_create_summary_table = """ CREATE TABLE IF NOT EXISTS summary_table(
                                        ID INTEGER PRIMARY KEY,
                                        LASTDUEL INTEGER,
                                        DUELSWON INTEGER DEFAULT 0,
                                        DUELSLOST INTEGER DEFAULT 0,
                                        DUELSTIED INTEGER DEFAULT 0,
                                        DAMAGEGIVEN INTEGER DEFAULT 0,
                                        DAMAGETAKEN INTEGER DEFAULT 0,
                                        MISSES INTEGER DEFAULT 0,
                                        HITSEVADED INTEGER DEFAULT 0,
                                        CRITS INTEGER DEFAULT 0,
                                        DAMAGECRIT INTEGER DEFAULT 0,
                                        MAXHIT INTEGER DEFAULT 0,
                                        MAXCRIT INTEGER DEFAULT 0
                                    ); """
    if db_file is not None:
        create_table(db_file, sql_create_summary_table)
    else:
        print('Error! Could not establish database connection.')


def create_user_id_table(db_file):
    """ create the initial database table user id
    Every user has a unique ID, a unique ID can be linked to multiple discord IDs
    :param db_file: database file
    :return: Nothing
    """
    sql_create_user_id_table = """ CREATE TABLE IF NOT EXISTS user_id_table(
                                        UNIQUEID INTEGER,
                                        USERID VARCHAR(20) PRIMARY KEY UNIQUE
                                    ); """
    if db_file is not None:
        create_table(db_file, sql_create_user_id_table)
    else:
        print('Error! Could not establish database connection.')


def create_duel_table(db_file):
    """ create the initial database table for duels
    :param db_file: database file
    :return: Nothing
    """
    sql_create_duel_table = """ CREATE TABLE IF NOT EXISTS duel_table(
                                        GUILD VARCHAR(20) ,
                                        CHANNEL VARCHAR(20) ,
                                        TRIGGERID VARCHAR(20) ,
                                        MESSAGEID VARCHAR(20) PRIMARY KEY UNIQUE,
                                        CHALLENGER VARCHAR(20) NOT NULL,
                                        CONTENDER VARCHAR(20) NOT NULL,
                                        DUELTEXT TEXT NOT NULL
                                    ); """
    if db_file is not None:
        create_table(db_file, sql_create_duel_table)
    else:
        print('Error! Could not establish database connection.')


def create_parsed_duel_table(db_file):
    """ create the parsed table for duels
    :param db_file: database file
    :return: Nothing
    """
    sql_create_parsed_duel_table = """ CREATE TABLE IF NOT EXISTS parsed_duel_table(
                                        MESSAGEID VARCHAR(20) PRIMARY KEY UNIQUE,
                                        CHALLENGER INT NOT NULL,
                                        CONTENDER INT NOT NULL,
                                        CHA_LVL VARCHAR(3) NOT NULL,
                                        CON_LVL VARCHAR(3) NOT NULL,
                                        CHA_DUELSTYLE VARCHAR(20) NOT NULL,
                                        CON_DUELSTYLE VARCHAR(20) NOT NULL,
                                        CHA_DAMAGE INT NOT NULL,
                                        CON_DAMAGE INT NOT NULL,
                                        CHA_EVADED INT NOT NULL,
                                        CON_EVADED INT NOT NULL,
                                        CHA_MAX_HIT INT NOT NULL,
                                        CON_MAX_HIT INT NOT NULL,
                                        CHA_CRIT INT NOT NULL,
                                        CON_CRIT INT NOT NULL,
                                        CHA_MAX_CRIT INT NOT NULL,
                                        CON_MAX_CRIT INT NOT NULL,
                                        WINNER VARCHAR(20) DEFAULT NULL
                                    ); """
    if db_file is not None:
        create_table(db_file, sql_create_parsed_duel_table)
    else:
        print('Error! Could not establish database connection.')


def create_duel_user_table(db_file):
    """ create the initial database table duel users
    :param db_file: database file
    :return: Nothing
    """
    sql_create_duel_user_table = """ CREATE TABLE IF NOT EXISTS duel_user_table(
                                        UNIQUEID INT PRIMARY KEY,
                                        DUELSWON INT DEFAULT 0,
                                        DUELSLOST INT DEFAULT 0,
                                        DUELSTIED INT DEFAULT 0,
                                        DAMAGEGIVEN VARCHAR(20) DEFAULT 0,
                                        DAMAGETAKEN VARCHAR(20) DEFAULT 0,
                                        MISSES VARCHAR(20) DEFAULT 0,
                                        HITSEVADED VARCHAR(20) DEFAULT 0,
                                        CRITS VARCHAR(20) DEFAULT 0,
                                        DAMAGECRIT VARCHAR(20) DEFAULT 0,
                                        MAXHIT VARCHAR(20) DEFAULT 0,
                                        MAXCRIT VARCHAR(20) DEFAULT 0,
                                        NEMESIS TEXT
                                    ); """
    if db_file is not None:
        create_table(db_file, sql_create_duel_user_table)
    else:
        print('Error! Could not establish database connection.')


def create_settings_table(db_file):
    """ create the initial database table for settings
    :param db_file: database file
    :return: Nothing
    """
    sql_create_settings_table = """ CREATE TABLE IF NOT EXISTS settings(
                                        NAME VARCHAR(20) PRIMARY KEY UNIQUE,
                                        SETTING VARCHAR(20) NOT NULL
                                    ); """
    if db_file is not None:
        create_table(db_file, sql_create_settings_table)
    else:
        print('Error! Could not establish database connection.')


def get_setting(conn, setting):
    """ Return member info
    :param conn: connection to db
    :param setting: setting to query
    :return: setting info or None
    """
    cur = conn.cursor(buffered=True)
    cur.execute('SELECT * FROM settings WHERE NAME=%s LIMIT 1', (str(setting),))
    conn.ping(True, 2)
    conn.commit()
    memberinfo = cur.fetchone()
    return memberinfo


def insert_setting(conn, name, setting):
    """ Insert new duel in db
    :param conn: connection to db
    :param name: setting's name
    :param setting: text for setting
    :return: last row ID
    """
    sql = ''' INSERT INTO settings(NAME, SETTING)
              VALUES(%s, %s) '''
    cur = conn.cursor(buffered=True)
    cur.execute(sql, [name, setting])
    conn.ping(True, 2)
    conn.commit()
    return cur.lastrowid


def update_setting(conn, name, setting):
    """ Insert new duel in db
    :param conn: connection to db
    :param name: setting's name
    :param setting: text for setting
    :return: last row ID
    """
    cur = conn.cursor(buffered=True)
    current_setting = get_setting(conn, name)
    if current_setting is None:
        insert_setting(conn, name, setting)
    else:
        sql = ''' UPDATE settings
                  SET SETTING = %s
                  WHERE NAME = %s'''
        cur.execute(sql, [setting, name])
        conn.ping(True, 2)
        conn.commit()
    return cur.lastrowid


def insert_member(conn, uid, memberid):
    """ Insert new duel in db
    :param conn: connection to db
    :param uid: unique ID
    :param memberid: user's ID
    :return: last row ID
    """
    sql = ''' INSERT INTO user_id_table(UNIQUEID, USERID)
              VALUES(%s, %s) '''
    cur = conn.cursor(buffered=True)
    cur.execute(sql, [str(uid), str(memberid)])
    conn.ping(True, 2)
    conn.commit()
    return cur.lastrowid


def get_member(conn, userid):
    """ Return member info
    :param conn: connection to db
    :param userid: member to query
    :return: memberinfo for user or None
    """
    cur = conn.cursor(buffered=True)
    cur.execute('SELECT * FROM user_id_table WHERE USERID=%s LIMIT 1', (str(userid),))
    conn.ping(True, 2)
    conn.commit()
    memberinfo = cur.fetchone()
    return memberinfo


def update_member(conn, userid):
    """ Return member info
    :param conn: connection to db
    :param userid: member to query
    :return: lastrowid
    """
    minfo = get_member(conn, userid)
    if minfo is None:
        count = get_membercount(conn)
        insert_member(conn, count+1, userid)
        return [count+1, userid]
    else:
        return minfo


def get_membercount(conn):
    """ Return member count
    :param conn: connection to db
    :return: memberinfo for user or None
    """
    cur = conn.cursor(buffered=True)
    cur.execute('SELECT MAX(UNIQUEID) FROM user_id_table')
    conn.ping(True, 2)
    conn.commit()
    membercount = cur.fetchone()
    return membercount[0]


def get_duel(conn, duelid):
    """ Return oneduel
    :param conn: connection to db
    :param duelid: duel to retrieve
    :return: duel or None
    """
    cur = conn.cursor(buffered=True)
    cur.execute('SELECT * FROM duel_table WHERE MESSAGEID=%s LIMIT 1', (duelid,))
    conn.ping(True, 2)
    conn.commit()
    duel = cur.fetchone()
    return duel


def get_stats(conn, userid):
    """ Return stats for user
    :param conn: connection to db
    :param userid: user in question
    :return: list of duels as contender and challenger
    """
    cur = conn.cursor(buffered=True)
    cur.execute('SELECT * FROM duel_table WHERE CONTENDER=%s LIMIT 1', (userid,))
    conn.ping(True, 2)
    conn.commit()
    contender = cur.fetchall()
    cur.execute('SELECT * FROM duel_table WHERE CHALLENGER=%s LIMIT 1', (userid,))
    conn.ping(True, 2)
    conn.commit()
    challenger = cur.fetchall()
    return contender, challenger


def get_last_duel(conn, channelid):
    """ Return oneduel
    :param conn: connection to db
    :param channelid: channel for which to retrieve duel
    :return: duel or None
    """
    cur = conn.cursor(buffered=True)
    cur.execute('SELECT * FROM duel_table WHERE CHANNEL=%s ORDER BY MESSAGEID DESC LIMIT 1 ', (channelid,))
    conn.ping(True, 2)
    conn.commit()
    duel = cur.fetchone()
    return duel


def get_duels(conn):
    """ Return all duels
    :param conn: connection to db
    :return: list of all duels or None
    """
    cur = conn.cursor(buffered=True)
    cur.execute('SELECT * FROM duel_table')
    conn.ping(True, 2)
    conn.commit()
    duels = cur.fetchall()
    return duels


def insert_duel(conn, duelinfo):
    """ Insert new duel in db
    :param conn: connection to db
    :param duelinfo: info to be inserted
    :return: last row ID or None
    """
    cur = conn.cursor(buffered=True)
    if get_duel(conn, duelinfo[3]) is None:
        sql = ''' INSERT INTO duel_table(GUILD, CHANNEL, TRIGGERID, MESSAGEID, CHALLENGER, CONTENDER, DUELTEXT)
                      VALUES(%s,%s,%s,%s,%s,%s,%s) '''
        cur.execute(sql, duelinfo)
        conn.ping(True, 2)
        conn.commit()
    return cur.lastrowid


def get_parsed_duel(conn, duelid):
    """ Return oneduel
    :param conn: connection to db
    :param duelid: parsed duel to retrieve
    :return: duel or None
    """
    cur = conn.cursor(buffered=True)
    cur.execute('SELECT * FROM parsed_duel_table WHERE MESSAGEID=%s LIMIT 1', (duelid,))
    conn.ping(True, 2)
    conn.commit()
    duel = cur.fetchone()
    return duel


def insert_parsed_duel(conn, duelinfo):
    """ Insert new duel in db
    :param conn: connection to db
    :param duelinfo: info to be inserted
    :return: last row ID or None
    """
    cur = conn.cursor(buffered=True)
    if get_parsed_duel(conn, duelinfo[0]) is None:
        sql = ''' INSERT INTO parsed_duel_table(MESSAGEID, CHALLENGER, CONTENDER, CHA_LVL, CON_LVL, 
        CHA_DUELSTYLE, CON_DUELSTYLE, CHA_DAMAGE, CON_DAMAGE, CHA_EVADED, CON_EVADED, CHA_MAX_HIT, CON_MAX_HIT,
        CHA_CRIT, CON_CRIT, CHA_MAX_CRIT, CON_MAX_CRIT, WINNER)
                      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '''
        cur.execute(sql, duelinfo)
        conn.ping(True, 2)
        conn.commit()
    return cur.lastrowid


def create_connection(db_info):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_info: host, user, pass, database
    :return: Connection object or None
    """
    try:
        conn = mariadb.connect(host=db_info[0], user=db_info[1], password=db_info[2], database=db_info[3])
        return conn
    except Exception as e:
        print(e)

    return None


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor(buffered=True)
        c.execute(create_table_sql)
    except Exception as e:
        print(e)
