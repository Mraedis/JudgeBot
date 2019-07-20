from discord.ext import commands
import asyncio
import atexit
import random as r
from jbsql import jbDB as j
# from jbsql import jbDBold as jo
import secret
import logging
import time


logging.basicConfig(level=logging.INFO, filename="logfile", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")

jBot = commands.Bot(command_prefix='&')
database = j.create_connection('judgeBot.db')

n2m = j.nick_to_member
m2n = j.member_to_nick

j.create_summary_table(database)
j.create_user_id_table(database)
j.create_duel_user_table(database)
j.create_duel_table(database)
# jo.create_old_duel_table(database)
# jo.create_old_duel_user_table(database)
sql_c = database.cursor()


@jBot.event
async def on_ready():
    logging.info('Logged in as')
    logging.info(jBot.user.name)
    logging.info(jBot.user.id)
    logging.info('------')
    for server in jBot.servers:
        if 'Runevillage' in server.name:
            logging.info('Updating membertable')
            update_membertable(server.members)


@jBot.event
async def on_server_join(server):
    logging.info('Joined the server: ' + server.name)


@jBot.event
async def on_member_update(before, after):
    if 'Runevillage' in before.server.name and before.nick != after.nick:
        logging.info('Member ' + before.name + ' is changing nicknames.')
        update_member(after)


@jBot.event
async def on_message(message):
    if ('JimmyBot#8050' in str(message.author)) and (' is dueling ' in message.content):
        logging.info('Attempting duel processing.')
        try:
            # get dueltext, save for later
            logging.info('---------------------------------')
            logging.info(message.content)
            logging.info('---------------------------------')
            # extract winner and use it to get contender and challenger
            # get userID for participants
            # decide to update stats table?
            # **Judge** (level 66, Rogue style) is dueling **Elmeric** (level 31, Normal style)!
            duellines = message.content.split('\n')

            challenger = (duellines[0].split('** ('))[0][2:]
            contender = ((duellines[0].split('** ('))[1]).split('is dueling **')[1]

            chanid = n2m(database, challenger)
            conid = n2m(database, contender)

            dueltext = message.content.replace('**', '')
            dueltext = dueltext.replace(contender, str(conid))
            dueltext = dueltext.replace(challenger, str(chanid))

            duelnum = j.insert_duel(database, [message.id, message.timestamp, chanid, conid, dueltext])
            if duelnum % 10 == 0:
                parse_all()
            logging.info('Just had duel number ' + str(duelnum) + '.')
        except Exception as e:
            logging.error('Caught an error in processing duels: ' + str(e))
    await jBot.process_commands(message)
    await asyncio.sleep(3)


@jBot.command(pass_context=True)
async def parse(ctx):
    """parse duelstats"""
    try:
        if '145451920557867008' in ctx.message.author.id:
            start = time.time()
            logging.info('Parsing duelstats')
            parse_all()
            stop = time.time()
            await ctx.send('Parsed the stats, took ' + str(round(stop - start, 3)) + ' seconds.')
        return
    except Exception as e:
        logging.error('Caught an error while trying to parse stats: ' + str(e))
        return await ctx.send('Something went wrong. Call 9-1-1-Judge!')


@jBot.command(pass_context=True)
async def execute(ctx):
    """parse duelstats"""
    try:
        if '145451920557867008' in ctx.message.author.id and 'order 66' in ctx.message.content:
            await ctx.send('It will be done, my lord.')
        return
    except Exception as e:
        logging.error('Caught an error while trying to exterminate jedi: ' + str(e))
        return await ctx.send('Something went wrong. Call 9-1-1-Judge!')


def parse_all():
    duels = j.get_duels(database)
    # duel user stats
    d_u_s = {}
    for duel in duels:
        chal = duel[2]
        cont = duel[3]
        # Add challenger if not already present
        if chal not in d_u_s:
            d_u_s[chal] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '']
        # Add contender if not already present
        if cont not in d_u_s:
            d_u_s[cont] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '']
        line1, line2 = duel[4].rsplit('\n', 1)
        for line in line1.split('\n'):
            if "'s HP: " in line:
                con_hit, cha_hit = line.split(', ')
                d_u_s[chal], d_u_s[cont] = parse_hits(d_u_s[chal], d_u_s[cont], cha_hit, con_hit)
        # Manage wincount
        con_hp = get_hp(cha_hit)
        cha_hp = get_hp(con_hit)
        if cha_hp > 0:
            d_u_s[chal][0] += 1
            d_u_s[cont][1] += 1
        elif con_hp > 0:
            d_u_s[cont][0] += 1
            d_u_s[chal][1] += 1
        else:
            d_u_s[chal][2] += 1
            d_u_s[cont][2] += 1
    # Send stats to database
    for user in d_u_s:
        stats = d_u_s[user]
        stats.insert(0, user)
        logging.info('Updating ' + str(user) + ' with stats: ' + str(stats))
        j.update_duel_user_stats(database, stats)


@jBot.command(pass_context=True)
@commands.cooldown(1, 30, commands.BucketType.user)
async def duelstats(ctx):
    """Gives a user their duelstats"""
    try:
        # USER TABLE:
        # USERID DUELSWON DUELSLOST DUELSTIED DAMAGEGIVEN DAMAGETAKEN
        # MISSES HITSEVADED CRITS DAMAGECRIT MAXHIT MAXCRIT NEMESIS
        us = ctx.message.author
        stats = j.get_stats(database, us.id)
        if us.nick is not None:
            name = us.nick
        else:
            name = us.name
        message = name + ' has won **' + str(round(stats[1] * 100 / (stats[1] + stats[2] + stats[3]))) + '**% (' \
                  + str(stats[1]) + '/' + str(stats[1] + stats[2] + stats[3]) + ') of their duels, excluding ' \
                  + str(stats[3]) + ' ties.\nThey managed to deal **' + str(stats[4]) + '** damage while taking **' \
                  + str(stats[5]) + '** damage in return.\nThey missed **' + str(stats[6]) \
                  + '** damage but managed to evade **' + str(stats[7]) \
                  + '** damage in return.\nTheir highest hit ever was **' + str(stats[10]) + '**.'
        return await ctx.send(message)
    except Exception as e:
        logging.error('Caught an error while trying to display parse SQL: ' + str(e))
        return await ctx.send('Something went wrong. Call 9-1-1-Judge!')


@jBot.command(pass_context=True)
@commands.cooldown(1, 15, commands.BucketType.user)
async def statme(ctx, arg1, arg2):
    """Attempts to parse any 'random' stat"""
    # statme top/bottom/avg stat
    try:
        # USER TABLE:
        # USERID DUELSWON DUELSLOST DUELSTIED DAMAGEGIVEN DAMAGETAKEN
        # MISSES HITSEVADED CRITS DAMAGECRIT MAXHIT MAXCRIT NEMESIS
        # Flavours: [[AVG/AVERAGE]]\\ MAX/TOP/HIGH/HIGHEST \\ LOW/BOTTOM/LOWEST/
        start = time.time()
        # us = ctx.message.author
        statmap = {
            "WON": 1, "WIN": 1, "WINS": 1,
            "LOSS": 2, "LOSSES": 2, "LOSE": 2,
            "TIES": 3, "TIED": 3, "DRAW": 3, "DRAWS": 3,
            "DMG": 4, "DAMAGE": 4,
            "MISS": 6, "MISSES": 6, "MISSED": 6,
            "EVADE": 7, "EVADED": 7, "EVADES": 7,
            "CRIT": 8, "CRITICAL": 8, "CRITS": 8,
            "HIT": 10, "HITS": 10, "HITTED": 10
        }
        arg1 = arg1.lower()
        arg2 = arg2.upper()
        if 'Runevillage' in str(ctx.message.server):
            if arg1 in 'tophighestbottomlowest' and arg2 in statmap:
                allstats = j.get_all_stats(database)
                topcalc = []
                orderasc = arg1 in 'tophighest'
                stat2calc = statmap[arg2]
                # dueluser
                logging.info('Got in argument. stat to calc is: ' + str(stat2calc))
                for du in allstats:
                    uname = m2n(database, du[0])
                    if uname is not None:
                        topcalc.append([uname, du[stat2calc]])
                    else:
                        logging.info('Could not parse userID ' + str(du[0]))
                listsorted = sorted(topcalc, key=lambda x: x[1], reverse=orderasc)
                message = 'Our ' + arg1 + ' 5 ' + arg2 + ' are:\n'
                for u in listsorted[0:5]:
                    message = message + '**{}** with **{}**\n'.format(u[0], str(u[1]))
                stop = time.time()
                logging.info('Calculating random duel stat took ' + str(stop-start) + ' seconds.')
            else:
                message = "I'm sorry I didn't understand that."
            await ctx.send(message)
        else:
            logging.info('Unauthorized command.')
        return
    except Exception as e:
        logging.error('Caught an error while trying parse statme: ' + str(e))
        return await ctx.send('Something went wrong. Call 9-1-1-Judge!')


# @jBot.command()
# @commands.cooldown(1, 30, commands.BucketType.server)
# async def lenny():
#     """shows one of the many lenny faces"""
#     lennyface = ["( ͡° ͜ʖ ͡°)", "(☭ ͜ʖ ☭)", "(ᴗ ͜ʖ ᴗ)", "( ° ͜ʖ °)", "(⟃ ͜ʖ ⟄)",
#                  "( ‾ ʖ̫ ‾)", "(͠≖ ͜ʖ͠≖)", "( ͡° ʖ̯ ͡°)", "ʕ ͡° ʖ̯ ͡°ʔ", "( ͡° ل͜ ͡°)",
#                  "( ͠° ͟ʖ ͡°)", "( ͠° ͟ʖ ͠°)", "( ͡~ ͜ʖ ͡°)", "( ͡o ͜ʖ ͡o)", "( ͡◉ ͜ʖ ͡◉)",
#                  "( ͡☉ ͜ʖ ͡☉)", "( ͡° ͜V ͡°)", "ʕ ͡° ͜ʖ ͡°ʔ", "( ͡ᵔ ͜ʖ ͡ᵔ )", "( ͡° ͜ʖ ͡ °)"]
#     try:
#         return await ctx.send(lennyface[r.randint(0, 19)])
#     except Exception as e:
#         logging.error('Caught an error while trying to display a Lenny Face: ' + str(e))
#         return await ctx.send('Something went wrong. Call 9-1-1-Judge!')
#
#
# @jBot.command()
# @commands.cooldown(1, 30, commands.BucketType.server)
# async def dangit():
#     """Puts up a dangit bobby image"""
#     try:
#         return await ctx.send('https://i.pinimg.com/564x/73/be/b2/73beb2a70497ca28a9e2d6d7b1056e9b.jpg')
#     except Exception as e:
#         logging.error('Caught an error while trying to do dangit bobby: ' + str(e))
#         return await ctx.send('Something went wrong. Call 9-1-1-Judge!')


@atexit.register
def database_close():
    logging.info('Goodbye.')
    database.close()


def update_membertable(memberlist):
    for member in memberlist:
        update_member(member)


def update_member(member):
    if member.nick:
        cnick = member.nick
    else:
        cnick = None
    memberinfo = j.get_member(database, member.id)
    prevnick = None
    if memberinfo is not None:
        numnicks = memberinfo[4]
        if member.nick and memberinfo[2] not in cnick and len(memberinfo[2]) != len(cnick):
            numnicks += 1
            prevnick = memberinfo[2]
    else:
        if cnick:
            numnicks = 1
        else:
            numnicks = 0
    update = [member.id, member.name, cnick, prevnick, numnicks]
    j.update_member(database, update)


def parse_hit(hit):
    damage, crit, evade = 0, 0, 0
    if ' » ' in hit:
        hit = hit.split(' » ')[1]
    else:
        hit = hit.split(' (', 1)[1]
    if '...' not in hit:
        damage = eval((hit.split(')')[0]).replace('x', '*'))
    if '*(CRIT!)*' in hit:
        crit = damage
    if '*(EVADED!)*' in hit:
        evade = damage
    return abs(damage), abs(crit), abs(evade)


def get_hp(hit):
    return int((hit.split("'s HP: ")[1]).split('/')[0])


def parse_hits(chal, cont, cha_hit, con_hit):
    # USERID DUELSWON DUELSLOST DUELSTIED DAMAGIVEN DAMAGETAKEN
    # MISSES HITSEVADED CRITS DAMAGECRIT MAXHIT MAXCRIT NEMESIS
    cha_dmg, cha_crit, con_ev = parse_hit(cha_hit)
    con_dmg, con_crit, cha_ev = parse_hit(con_hit)
    chal = calc_hits(chal, cha_dmg, cha_crit, cha_ev, con_dmg, con_ev)
    cont = calc_hits(cont, con_dmg, con_crit, con_ev, cha_dmg, cha_ev)
    return chal, cont


def calc_hits(stats, dmg, crit, evade, taken, missed):
    # WON LOST TIED GIVEN TAKEN MISS EVADE CRIT CRITDMG MAXHIT MAXCRIT NEMESIS
    # 0   1    2    3     4     5    6     7    8       9      10      11
    # [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '']
    stats[3] += dmg
    stats[4] += taken
    stats[5] += missed
    stats[6] += evade
    stats[7] += stats[8] > 0
    stats[8] += crit
    if dmg > stats[9]:
        stats[9] = dmg
    if crit > stats[10]:
        stats[10] = crit
    return stats


@jBot.command(pass_context=True)
async def dueltop(ctx):
    """parses  all stats for top"""
    try:
        start = time.time()
        us = ctx.message.author
        if '145451920557867008' in us.id:
            allstats = j.get_all_stats(database)
            topcalc = []
            # dueluser
            for du in allstats:
                uname = m2n(database, du[0])
                if uname is not None:
                    # User, win%,
                    topcalc.append([uname, str(round(du[1] * 100 / (du[1] + du[2] + du[3]))),
                                    du[1], str(du[1] + du[2] + du[3]), str(du[3]), du[10]])
                else:
                    logging.info('Could not parse userID ' + str(du[0]))
                winsorted = sorted(topcalc, key=lambda x: x[2], reverse=True)
                hitsorted = sorted(topcalc, key=lambda x: x[5], reverse=True)
                message = 'Our top 5 **!duel winners** are:\n'

                for u in winsorted[0:5]:
                    message = message + '**{}** with **{}**% won ({}/{} and {} ties.)\n'\
                        .format(u[0], u[1], str(u[2]), u[3], u[4])

                message = message + '\nLooking just at the **heaviest hits:**\n'

                for u in hitsorted[0:5]:
                    message = message + '**{}** hit a max of **{}**.\n'.format(u[0], str(u[5]))
            stop = time.time()
            await ctx.send(message)
            logging.info('Calculating top duel stats took ' + str(stop-start) + ' seconds.')
        else:
            logging.info('Unauthorized command.')
        return
    except Exception as e:
        logging.error('Caught an error while trying to calculate top: ' + str(e))
        return await ctx.send('Something went wrong. Call 9-1-1-Judge!')


@jBot.command(pass_context=True)
async def oldstats(ctx):
    """parses gives a user duelstats"""
    try:
        start = time.time()
        us = ctx.message.author
        if '145451920557867008' in us.id and 'Runevillage' in ctx.message.server.name:
            logging.info('Starting parsing of old stats')
            targetchan = jBot.get_channel('208498021078401025')
            afmsg = await jBot.get_message(targetchan, '212062234325680128')  # 212062174623825921
            targetmsg = 442022192054665216
            done = 0
            duelid = 0
            usertable = {}
            while int(afmsg.id) <= targetmsg:
                async for msg in jBot.logs_from(targetchan, after=afmsg, limit=5000):
                    if ('JimmyBot' in str(msg.author)) and (' is dueling ' in msg.content):
                        duellines = msg.content.split('\n')
                        conid = '000000000000000000'
                        chalid = '000000000000000000'

                        if '**' in duellines[0]:
                            chal = (duellines[0].split('** ('))[0][2:]
                            cont = ((duellines[0].split('** ('))[1]).split('is dueling **')[1]
                        else:
                            chal = (duellines[0].split(' ('))[0]
                            cont = ((duellines[0].split(' ('))[1]).split('is dueling ')[1]

                        async for trig in jBot.logs_from(targetchan, before=msg, limit=15, reverse=True):
                            if '!duel' in trig.content and trig.mentions:
                                chalid = trig.author.id
                                conid = trig.mentions[0].id
                                break
                        dueltext = msg.content.replace('**', '')
                        dueltext = dueltext.replace(cont, conid)
                        dueltext = dueltext.replace(chal, chalid)
                        duelid = j.insert_duel(database, [msg.id, msg.timestamp, chalid, conid, dueltext])
                afmsg = msg
                done += 1
                if done % 5 == 0:
                    logging.info(str(5000 * done) + ' messages done @ msgID ' + str(afmsg.id)
                                 + ', last duelid: ' + str(duelid) + '.')
            stop = time.time()
            logging.info(usertable)
            await ctx.send('Parsed the old stats, took ' + str(round(stop - start, 3)) + ' seconds.')
        else:
            logging.info('Unauthorized command.')
        return
    except Exception as e:
        logging.error('Caught an error while trying to parse stats: ' + str(e))
        return await ctx.send('Something went wrong. Call 9-1-1-Judge!')

jBot.run(secret.secret)
