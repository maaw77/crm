import logging

import asyncio
import asyncpg
import aiohttp

from datetime import datetime, date, timedelta, time

import sys
from pathlib import Path

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.paren

# Importing project packages.
from settings_management import Settings
from dbase.data_validators import GsmTable, deserial_valid
from dbase import database
import web
from filestools import fileapi


async def bot_gsm_table(session: aiohttp.ClientSession, pool: asyncpg.Pool,  sleep_time: float = 0.0):
    statement = 'SELECT id  FROM gsm_table WHERE guid=$1 AND income_kg=$2;'
    while True:
        await asyncio.sleep(sleep_time)
        next_day_time = datetime.combine(date.today() + timedelta(days=1), time(23, 0))
        logging.info('Start.')
        # await web.login_user(session)
        async with pool.acquire() as conn:
            async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[3]):
                async for data in deserial_valid(raw_data, GsmTable):
                    data: GsmTable
                    record: asyncpg.Record = await conn.fetchrow(statement, data.guid, data.income_kg)
                    if record is None:
                        await database.insert_gsm_table(conn, data)
        delta_t = next_day_time - datetime.now()
        sleep_time = delta_t.total_seconds()
        logging.info(f"Stop (sleep time = {delta_t}).")


async def main():
    con_par = Settings()  # Loading environment variables
    async with asyncpg.create_pool(host=con_par.HOST_DB,
                                   port=con_par.PORT_DB,
                                   user=con_par.POSTGRES_USER,
                                   database=con_par.POSTGRES_DB,
                                   password=con_par.POSTGRES_PASSWORD.get_secret_value()) as pool:
        jar = aiohttp.CookieJar(unsafe=True)
        async with aiohttp.ClientSession(headers=web.HEADERS, cookie_jar=jar) as session:
            await bot_gsm_table(session, pool)



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    asyncio.run(main())

    # today = date.today()
    # print(today)
    # next_day = today + timedelta(days=1)
    # print(next_day)
    # next_day_time = datetime.combine(next_day, time(23, 0))
    # print(next_day_time)
    # delta_t = next_day_time - datetime.now()
    # print(delta_t.total_seconds())
    #
    # next_day_time = datetime.combine(date.today() + timedelta(days=1), time(23, 0))
    # pass
    # delta_t = next_day_time - datetime.now()
    # print(delta_t.total_seconds())
