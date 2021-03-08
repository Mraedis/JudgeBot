import os
import discord
from discord.ext import commands
import logging
import atexit
import dbcommands as d


logging.basicConfig(level=logging.INFO, filename="logfile", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")


token = os.getenv("DISCORD_TOKEN")
db_host = os.getenv("DB_HOST")
db_pass = os.getenv("DB_PASS")
db_user = os.getenv("DB_USER")
db_name = os.getenv("DB_NAME")


intents = discord.Intents.default()
bot = commands.Bot(command_prefix='&', intents=intents)
database = d.create_connection([db_host, db_user, db_pass, db_name])


@bot.event
async def on_ready():
    logging.info('Logged in as')
    logging.info(bot.user)
    for guild in bot.guilds:
        logging.info('Bot connected to ' + guild.name + '(' + str(guild.id) + ')')


@atexit.register
def database_close():
    logging.info('Goodbye.')
    database.close()


bot.run(token)
