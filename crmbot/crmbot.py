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
from dbase import data_validators, database
import web
from filestools import fileapi


async def bot_gsm_table(session: aiohttp.ClientSession, pool: asyncpg.Pool, sleep_time: float = 0.0):
    while True:
        await asyncio.sleep(sleep_time)
        next_day_time = datetime.combine(date.today() + timedelta(days=1), time(23, 0))
        logging.info('Start.')
        # await web.login_user(session)
        async with pool.acquire() as conn:
            async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[3]):
            # async for raw_data in web.fetch_table(web.webapi.URL_GSM_TABLE, session):
                async for data in data_validators.deserial_valid(raw_data, data_validators.GsmTable):
                    await database.insert_gsm_table(conn, data)  # ???? insert_gsm_tabl Return Coroutine [ANY, ANY, INT]

        delta_t = next_day_time - datetime.now()
        if delta_t > timedelta(days=1):
            delta_t -= timedelta(days=1)
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


