import sqlite3


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
    :param db_file: database file
    :return: Nothing
    """
    sql_create_user_id_table = """ CREATE TABLE IF NOT EXISTS user_id_table(
                                        USERID LONG INTEGER PRIMARY KEY,
                                        NAME TEXT,
                                        NICK TEXT,
                                        PREVIOUSNICK TEXT,
                                        PASTNICKS INTEGER DEFAULT 0
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
                                        DUELID LONG INTEGER PRIMARY KEY,
                                        TIMESTAMP DECIMAL,
                                        CHALLENGER LONG INTEGER NOT NULL,
                                        CONTENDER LONG INTEGER NOT NULL,
                                        DUELTEXT TEXT NOT NULL
                                    ); """
    if db_file is not None:
        create_table(db_file, sql_create_duel_table)
    else:
        print('Error! Could not establish database connection.')


def create_duel_user_table(db_file):
    """ create the initial database table duel users
    :param db_file: database file
    :return: Nothing
    """
    sql_create_duel_user_table = """ CREATE TABLE IF NOT EXISTS duel_user_table(
                                        USERID LONG INTEGER PRIMARY KEY,
                                        DUELSWON LONG INTEGER DEFAULT 0,
                                        DUELSLOST LONG INTEGER DEFAULT 0,
                                        DUELSTIED LONG INTEGER DEFAULT 0,
                                        DAMAGEGIVEN LONG INTEGER DEFAULT 0,
                                        DAMAGETAKEN LONG INTEGER DEFAULT 0,
                                        MISSES LONG INTEGER DEFAULT 0,
                                        HITSEVADED LONG INTEGER DEFAULT 0,
                                        CRITS LONG INTEGER DEFAULT 0,
                                        DAMAGECRIT LONG INTEGER DEFAULT 0,
                                        MAXHIT LONG INTEGER DEFAULT 0,
                                        MAXCRIT LONG INTEGER DEFAULT 0,
                                        NEMESIS TEXT
                                    ); """
    if db_file is not None:
        create_table(db_file, sql_create_duel_user_table)
    else:
        print('Error! Could not establish database connection.')


def get_duel(conn, duelid):
    """ Return oneduel
    :param conn: connection to db
    :param duelid: duel to retrieve
    :return: duel or None
    """
    cur = conn.cursor()
    cur.execute('SELECT * FROM duel_table WHERE DUELID=? LIMIT 1', (duelid,))
    conn.commit()
    duel = cur.fetchone()
    return duel


def get_duels(conn):
    """ Return all duels
    :param conn: connection to db
    :return: list of all duels or None
    """
    cur = conn.cursor()
    cur.execute('SELECT * FROM duel_table')
    conn.commit()
    duels = cur.fetchall()
    return duels


def insert_duel(conn, duelinfo):
    """ Insert new duel in db
    :param conn: connection to db
    :param duelinfo: info to be inserted
    :return: last row ID or None
    """
    cur = conn.cursor()
    if get_duel(conn, duelinfo[0]) is None:
        sql = ''' INSERT INTO duel_table(DUELID, TIMESTAMP, CHALLENGER, CONTENDER, DUELTEXT)
                      VALUES(?,?,?,?,?) '''
        cur.execute(sql, duelinfo)
        conn.commit()
    return cur.lastrowid


def nick_to_member(conn, nick):
    """ Get member from (nick)name
    :param conn: connection to db
    :param nick: nickname
    :return: membername or None
    """
    cur = conn.cursor()
    cur.execute('SELECT * FROM user_id_table WHERE NICK=? LIMIT 1', (nick,))
    conn.commit()
    memberinfo = cur.fetchone()
    if memberinfo is not None:
        return memberinfo[0]
    else:
        cur.execute('SELECT * FROM user_id_table WHERE NAME=? LIMIT 1', (nick,))
        conn.commit()
        memberinfo = cur.fetchone()
        if memberinfo is not None:
            return memberinfo[0]
        else:
            return None


def member_to_nick(conn, userid):
    """ Get (nick)name from userid
    :param conn: connection to db
    :param userid: users discord ID
    :return: (nick)name or None
    """
    cur = conn.cursor()
    cur.execute('SELECT * FROM user_id_table WHERE USERID=? LIMIT 1', (userid,))
    conn.commit()
    memberinfo = cur.fetchone()
    if memberinfo is not None:
        if memberinfo[2] is not None:
            return memberinfo[2]
        else:
            return memberinfo[1]
    else:
        return None


def get_member(conn, member):
    """ Return member info
    :param conn: connection to db
    :param member: member to query
    :return: memberinfo for user or None
    """
    cur = conn.cursor()
    cur.execute('SELECT * FROM user_id_table WHERE USERID=? LIMIT 1', (member,))
    conn.commit()
    memberinfo = cur.fetchone()
    return memberinfo


def insert_member(conn, memberinfo):
    """ Insert new duel in db
    :param conn: connection to db
    :param memberinfo: member to be inserted
    :return: last row ID
    """
    sql = ''' INSERT INTO user_id_table(USERID, NAME, NICK, PREVIOUSNICK, PASTNICKS)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, memberinfo)
    conn.commit()
    return cur.lastrowid


def update_member(conn, memberinfo):
    """
    update priority, begin_date, and end date of a task
    :param conn:
    :param memberinfo:
    """
    cur = conn.cursor()
    cur_memberinfo = get_member(conn, memberinfo[0])
    if cur_memberinfo is None:
        insert_member(conn, memberinfo)
        print('Member ' + memberinfo[0] + ' created.')

    sql = ''' UPDATE user_id_table
                  SET NAME = ? ,
                      NICK = ? ,
                      PREVIOUSNICK = ? ,
                      PASTNICKS = ?
                  WHERE USERID = ?'''
    membercute = memberinfo[1:]
    membercute.append(memberinfo[0])
    cur.execute(sql, membercute)
    conn.commit()


def get_stats(conn, member):
    """ Return duelstats for member
    :param conn: connection to db
    :param member: member to be retrieved
    :return: stats or None
    """
    cur = conn.cursor()
    cur.execute('SELECT * FROM duel_user_table WHERE USERID=? LIMIT 1', (member,))
    conn.commit()
    memberstats = cur.fetchone()
    return memberstats


def get_all_stats(conn):
    """ Return all duels
    :param conn: connection to db
    :return: list of all duel stats or None
    """
    cur = conn.cursor()
    cur.execute('SELECT * FROM duel_user_table')
    conn.commit()
    stats = cur.fetchall()
    return stats


def insert_duel_user_stats(conn, statinfo):
    """ Insert new duel user statsin db
    :param conn: connection to db
    :param statinfo: stats to be inserted
    """
    sql = ''' INSERT INTO duel_user_table(USERID, DUELSWON, DUELSLOST, DUELSTIED, DAMAGEGIVEN, DAMAGETAKEN, MISSES, HITSEVADED, CRITS, DAMAGECRIT, MAXHIT, MAXCRIT, NEMESIS)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, statinfo)
    conn.commit()


def update_duel_user_stats(conn, statinfo):
    """ Update duel user stats
    :param conn: connection to db
    :param statinfo: stats to be inserted
    """
    stats = get_stats(conn, statinfo[0])
    cur = conn.cursor()
    if stats is not None:
        sql = '''UPDATE duel_user_table
                  SET DUELSWON = ? ,
                      DUELSLOST = ? ,
                      DUELSTIED = ? ,
                      DAMAGEGIVEN = ? ,
                      DAMAGETAKEN = ? ,
                      MISSES = ? ,
                      HITSEVADED = ? ,
                      CRITS = ? ,
                      DAMAGECRIT = ? ,
                      MAXHIT = ? ,
                      MAXCRIT = ? ,
                      NEMESIS = ?
                  WHERE USERID = ?'''
        update = statinfo[1:]
        update.append(statinfo[0])
        cur.execute(sql, update)
    else:
        insert_duel_user_stats(conn, statinfo)
    conn.commit()


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
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
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        print(e)

