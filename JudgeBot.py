from discord.ext import commands
import atexit
from jbsql import jbDB as j
import secret
import asyncio
import logging
import time

logging.basicConfig(level=logging.INFO, filename="logfile", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")

jBot = commands.Bot(command_prefix='&')

database = j.create_connection(secret.db_info)

j.create_summary_table(database)
j.create_user_id_table(database)
j.create_duel_user_table(database)
j.create_duel_table(database)
j.create_parsed_duel_table(database)
j.create_settings_table(database)
sql_c = database.cursor()


@jBot.event
async def on_ready():
    logging.info('Logged in as')
    logging.info(jBot.user.name)
    logging.info(jBot.user.id)
    logging.info('------')
    for guild in jBot.guilds:
        update_membertable(guild)


@atexit.register
def database_close():
    logging.info('Goodbye.')
    database.close()


def update_membertable(guild):
    for member in guild.members:
        if (not member.bot) and (j.get_member(database, member.id) is None):
            insert_member(database, member)


def insert_member(db, member):
    count = j.get_membercount(db)
    if count is None:
        j.insert_member(db, 1, member.id)
    else:
        j.insert_member(db, int(count)+1, member.id)


def parse_hit(hit):
    damage, crit, evade = 0, 0, 0
    if " » " in hit:
        hit = hit.split(" » ")[1]
    else:
        hit = hit.split(" (", 1)[1]
    if "..." not in hit:
        damage = eval((hit.split(")")[0]).replace("x", "*"))
    if "*(CRIT!)*" in hit:
        crit = damage
    if "*(EVADED!)*" in hit:
        evade = damage
    return abs(damage), abs(crit), abs(evade)


def get_hp(hit):
    return int(((hit.split("'s HP: ")[1]).split("/")[0]).split("**")[1])


def parse_hits(chal, cont, cha_hit, con_hit):
    # USERID DUELSWON DUELSLOST DUELSTIED DAMAGIVEN DAMAGETAKEN
    # MISSES HITSEVADED CRITS DAMAGECRIT MAXHIT MAXCRIT NEMESIS
    cha_dmg, cha_crit, con_ev = parse_hit(cha_hit)
    con_dmg, con_crit, cha_ev = parse_hit(con_hit)
    chal = calc_hits(chal, cha_dmg, cha_crit, cha_ev)
    cont = calc_hits(cont, con_dmg, con_crit, con_ev)
    return chal, cont


def calc_hits(stats, dmg, crit, evade):
    # GIVEN EVADED CRITDMG MAXHIT MAXCRIT
    #   0      1      2       3      4
    #  [0, 0, 0, 0, 0, 0, 0]
    stats[0] += dmg
    stats[1] += evade
    stats[2] += crit
    if dmg > stats[3]:
        stats[3] = dmg
    if crit > stats[4]:
        stats[4] = crit
    return stats


def parse_header(header):
    if 'style' in header:
        cha_lvl = header.split(" is dueling ")[0].split(", ")[0].split("level ")[1]
        con_lvl = header.split(" is dueling ")[1].split(", ")[0].split("level ")[1]
        cha_style = header.split(" is dueling ")[0].split(", ")[1].split(" style")[0]
        con_style = header.split(" is dueling ")[1].split(", ")[1].split(" style")[0]
    else:
        cha_lvl = header.split(" is dueling ")[0].split("level ")[1].split(")")[0]
        con_lvl = header.split(" is dueling ")[1].split("level ")[1].split(")")[0]
        cha_style = "None"
        con_style = "None"
    return cha_lvl, con_lvl, cha_style, con_style


def parse_all():
    duels = j.get_duels(database)
    # duel user stats
    d_u_s = {}
    lastparsed = None
    for duel in duels:
        try:
            dueltext = duel[6]
            winner = 0
            chal = j.update_member(database, duel[4])[0]
            cont = j.update_member(database, duel[5])[0]
            if "**" in dueltext.split('\n', 1)[0]:
                repl_chal = dueltext.split(" is dueling ")[0].split("**", 1)[1].rsplit("**")[0]
                repl_con = dueltext.split('\n', 1)[0].split(" is dueling ")[1].split("**", 1)[1].rsplit("**")[0]
            else:
                repl_chal = dueltext.split(") is dueling ")[0].rsplit(" (", 1)[0]
                repl_con = dueltext.split('\n', 1)[0].split(") is dueling ")[1].rsplit(" (", 1)[0]
            dueltext = dueltext.replace(repl_chal, "CHALLENGER")
            dueltext = dueltext.replace(repl_con, "CONTENDER")
            # GIVEN EVADED CRITDMG MAXHIT MAXCRIT
            #   0     1      2       3       4
            # [0, 0, 0, 0, 0, 0]
            d_u_s[chal] = [0, 0, 0, 0, 0]
            d_u_s[cont] = [0, 0, 0, 0, 0]
            line1, line2 = dueltext.rsplit('\n', 1)
            cha_lvl, con_lvl, cha_style, con_style = parse_header(line1.split('\n')[0])
            for line in line1.split('\n'):
                if "'s HP: " in line:
                    con_hit, cha_hit = line.split(", ")
                    d_u_s[chal], d_u_s[cont] = parse_hits(d_u_s[chal], d_u_s[cont], cha_hit, con_hit)
            # Manage wincount
            con_hp = get_hp(cha_hit)
            cha_hp = get_hp(con_hit)
            if cha_hp > 0:
                winner = chal
            elif con_hp > 0:
                winner = cont
            else:
                winner = 0
            # Send stats to database
            # INSERT INTO parsed_duel_table(MESSAGEID, CHALLENGER, CONTENDER, CHA_LVL, CON_LVL,
            # CHA_DUELSTYLE, CON_DUELSTYLE, CHA_DAMAGE, CON_DAMAGE, CHA_EVADED, CON_EVADED,
            # CHA_MAX_HIT, CON_MAX_HIT, CHA_CRIT, CON_CRIT, CHA_MAX_CRIT, CON_MAX_CRIT, WINNER)
            # print(str([duel[0], chal, cont, cha_lvl, con_lvl, cha_style, con_style, d_u_s[chal][0], d_u_s[cont][0],
            #           d_u_s[chal][1], d_u_s[cont][1], d_u_s[chal][3], d_u_s[cont][3], d_u_s[chal][2], d_u_s[cont][2],
            #           d_u_s[chal][4], d_u_s[cont][4], winner])
            j.insert_parsed_duel(database, [duel[3], chal, cont, cha_lvl, con_lvl, cha_style, con_style,
                                            d_u_s[chal][0], d_u_s[cont][0], d_u_s[chal][1], d_u_s[cont][1],
                                            d_u_s[chal][3], d_u_s[cont][3], d_u_s[chal][2], d_u_s[cont][2],
                                            d_u_s[chal][4], d_u_s[cont][4], winner])
            lastparsed = duel[3]
        except Exception as e:
            logging.error('Could not process duel with ID ' + str(duel[3]) + ', error: ' + str(e))
    j.update_setting(database, "last_duel", str(lastparsed))


def parse_one(duelid):
    if len(duelid) > 1:
        duel = duelid
    else:
        duel = j.get_duel(database, duelid)
    # duel user stats
    d_u_s = {}
    try:
        dueltext = duel[6]
        winner = 0
        chal = j.update_member(database, duel[4])[0]
        cont = j.update_member(database, duel[5])[0]
        if "**" in dueltext.split('\n', 1)[0]:
            repl_chal = dueltext.split(" is dueling ")[0].split("**", 1)[1].rsplit("**")[0]
            repl_con = dueltext.split('\n', 1)[0].split(" is dueling ")[1].split("**", 1)[1].rsplit("**")[0]
        else:
            repl_chal = dueltext.split(") is dueling ")[0].rsplit(" (", 1)[0]
            repl_con = dueltext.split('\n', 1)[0].split(") is dueling ")[1].rsplit(" (", 1)[0]
        dueltext = dueltext.replace(repl_chal, "CHALLENGER")
        dueltext = dueltext.replace(repl_con, "CONTENDER")
        # GIVEN EVADED CRITDMG MAXHIT MAXCRIT
        #   0     1      2       3       4
        # [0, 0, 0, 0, 0, 0]
        d_u_s[chal] = [0, 0, 0, 0, 0]
        d_u_s[cont] = [0, 0, 0, 0, 0]
        line1, line2 = dueltext.rsplit('\n', 1)
        cha_lvl, con_lvl, cha_style, con_style = parse_header(line1.split('\n')[0])
        for line in line1.split('\n'):
            if "'s HP: " in line:
                con_hit, cha_hit = line.split(", ")
                d_u_s[chal], d_u_s[cont] = parse_hits(d_u_s[chal], d_u_s[cont], cha_hit, con_hit)
        # Manage wincount
        con_hp = get_hp(cha_hit)
        cha_hp = get_hp(con_hit)
        if cha_hp > 0:
            winner = chal
        elif con_hp > 0:
            winner = cont
        else:
            winner = 0
        # Send stats to database
        # INSERT INTO parsed_duel_table(MESSAGEID, CHALLENGER, CONTENDER, CHA_LVL, CON_LVL,
        # CHA_DUELSTYLE, CON_DUELSTYLE, CHA_DAMAGE, CON_DAMAGE, CHA_EVADED, CON_EVADED,
        # CHA_MAX_HIT, CON_MAX_HIT, CHA_CRIT, CON_CRIT, CHA_MAX_CRIT, CON_MAX_CRIT, WINNER)
        # print(str([duel[0], chal, cont, cha_lvl, con_lvl, cha_style, con_style, d_u_s[chal][0], d_u_s[cont][0],
        #           d_u_s[chal][1], d_u_s[cont][1], d_u_s[chal][3], d_u_s[cont][3], d_u_s[chal][2], d_u_s[cont][2],
        #           d_u_s[chal][4], d_u_s[cont][4], winner])
        j.insert_parsed_duel(database, [duel[3], chal, cont, cha_lvl, con_lvl, cha_style, con_style,
                                        d_u_s[chal][0], d_u_s[cont][0], d_u_s[chal][1], d_u_s[cont][1],
                                        d_u_s[chal][3], d_u_s[cont][3], d_u_s[chal][2], d_u_s[cont][2],
                                        d_u_s[chal][4], d_u_s[cont][4], winner])
        lastparsed = duel[3]
    except Exception as e:
        logging.error('Could not process duel with ID ' + str(duel[3]) + ', error: ' + str(e))


@jBot.event
async def on_message(message):
    if ('JimmyBot#8050' in str(message.author)) and (' is dueling ' in message.content):
        try:
            if message.channel.permissions_for(message.guild.me).read_messages:
                async for prevmsg in message.channel.history(before=message.created_at, limit=1):
                    if prevmsg.mentions:
                        j.insert_duel(database, [message.guild.id, message.channel.id, message.id, prevmsg.id,
                                                 prevmsg.author.id, prevmsg.mentions[0].id, message.content])
                        parse_one([message.guild.id, message.channel.id, message.id, prevmsg.id,
                                   prevmsg.author.id, prevmsg.mentions[0].id, message.content])
                    else:
                        logging.info('No mention was found in the trigger message')
        except Exception as e:
            logging.error('Caught an error in processing duels: ' + str(e))
    await jBot.process_commands(message)
    await asyncio.sleep(3)


@jBot.command(pass_context=True)
async def oldstats(ctx):
    """parses the old messages for duels"""
    afmsg = None
    targettime = None
    tid = 212062557156933641
    try:
        start = time.time()
        us = ctx.message.author
        if 145451920557867008 == us.id and 'Runevillage' in ctx.message.guild.name:
            logging.info('Starting parsing of old stats')
            tchan = jBot.get_channel(208498021078401025)
            # 212062557156933641 212062557156933641 489755037304750081
            tmsg = await tchan.fetch_message(699104265763028992)
            targettime = tmsg.created_at
            for targetchan in ctx.message.guild.text_channels:
                if targetchan.permissions_for(ctx.message.guild.me).read_messages:
                    if targetchan.id == 208498021078401025:
                        tmsg = await tchan.fetch_message(702541070801829950)
                        targettime = tmsg.created_at
                    else:
                        tchan = jBot.get_channel(208498021078401025)
                        tmsg = await tchan.fetch_message(212062557156933641)
                        targettime = tmsg.created_at
                    prevmsg = None
                    done = 0
                    trigger = False
                    ending = targetchan.last_message_id
                    print(str(ending))
                    print(str(targettime))
                    print(str(targetchan.name))
                    if ending is not None:
                        while tid < ending:
                            async for msg in targetchan.history(after=targettime, limit=5000):
                                if '!duel ' in msg.content:
                                    prevmsg = msg
                                    trigger = True
                                elif (msg.author.id == 209166316035244033) and (' is dueling ' in msg.content) and trigger:
                                    # GUILD, CHANNEL, TRIGGERID, MESSAGEID, CHALLENGER, CONTENDER, DUELTEXT
                                    j.insert_duel(database, [ctx.message.guild.id, targetchan.id, msg.id, prevmsg.id,
                                                             prevmsg.author.id, prevmsg.mentions[0].id, msg.content])
                                    trigger = False
                                else:
                                    trigger = False
                            afmsg = msg
                            tid = afmsg.id
                            done += 1
                            targettime = afmsg.created_at
                            if done % 5 == 0:
                                logging.info(str(5000 * done) + ' messages done @ msgID ' + str(afmsg.id))
            stop = time.time()
            await ctx.send('Parsed the old stats, took ' + str(round(stop - start, 3)) + ' seconds.')
        else:
            logging.info('Unauthorized command.')
        return
    except Exception as e:
        logging.error('Caught an error while trying to parse stats: ' + str(e))
        print(str(afmsg.id))
        print(str(targettime))
        return await ctx.send('Something went wrong. Call 9-1-1-Judge!')

jBot.run(secret.secret)
