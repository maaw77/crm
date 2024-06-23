import logging

import asyncio
import asyncpg
import aiohttp

from datetime import datetime, date, timedelta, time
from collections.abc import AsyncIterable, Callable
from typing import Union, Type

import sys
from pathlib import Path

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.paren

# Importing project packages.
from settings_management import Settings
from dbase import database
from dbase.data_validators import (GsmTable, TankTable, SheetTable, AZSTable,
                                   ExchangeTable, RemainsTable, deserial_valid)
import web
from filestools import fileapi


# async def bot_gsm_table(session: aiohttp.ClientSession, pool: asyncpg.Pool, sleep_time: float = 0.0):
#     while True:
#         await asyncio.sleep(sleep_time)
#         next_day_time = datetime.combine(date.today() + timedelta(days=1), time(23, 0))
#         logging.info('Start.')
#         # await web.login_user(session)
#         async with pool.acquire() as conn:
#             async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[3]):
#             # async for raw_data in web.fetch_table(web.webapi.URL_GSM_TABLE, session):
#                 async for data in data_validators.deserial_valid(raw_data, data_validators.GsmTable):
#                     await database.insert_gsm_table(conn, data)  # ???? insert_gsm_tabl Return Coroutine [ANY, ANY, INT]
#
#         delta_t = next_day_time - datetime.now()
#         if delta_t > timedelta(days=1):
#             delta_t -= timedelta(days=1)
#         sleep_time = delta_t.total_seconds()
#         logging.info(f"Stop (sleep time = {delta_t}).")


async def bot_table(session: aiohttp.ClientSession,
                    pool: asyncpg.Pool, *,
                    fetcher: AsyncIterable[str],
                    insert_table: Callable,
                    validator: Union[Type[GsmTable], Type[TankTable], Type[SheetTable],
                                     Type[AZSTable], Type[ExchangeTable], Type[RemainsTable]],
                    sleep_time: float = 0.0):
    """
    Scans the endpoints of the web application and saves new data (resources) to the database.
    Automatically starts scanning at 23:00 every day.
    """
    while True:
        await asyncio.sleep(sleep_time)
        next_day_time = datetime.combine(date.today() + timedelta(days=1), time(23, 0))
        logging.info(f'Start({validator}).')
        try:
            # await web.login_user(session)
            async with pool.acquire() as conn:
                async for raw_data in fetcher:
                    async for data in deserial_valid(raw_data, validator):
                        await insert_table(conn, data)
        except (AssertionError, asyncio.TimeoutError, aiohttp.ClientConnectionError) as er:
            logging.error(f'Bot ({validator}) connection error({er})!')

        delta_t = next_day_time - datetime.now()
        if delta_t > timedelta(days=1):
            delta_t -= timedelta(days=1)
        sleep_time = delta_t.total_seconds()
        logging.info(f'Stop (sleep ({validator}) time = {delta_t}).')


async def main():
    con_par = Settings()  # Loading environment variables
    async with asyncpg.create_pool(host=con_par.HOST_DB,
                                   port=con_par.PORT_DB,
                                   user=con_par.POSTGRES_USER,
                                   database=con_par.POSTGRES_DB,
                                   password=con_par.POSTGRES_PASSWORD.get_secret_value()) as pool:
        jar = aiohttp.CookieJar(unsafe=True)
        session_timeout = aiohttp.ClientTimeout(total=10, connect=5)
        async with aiohttp.ClientSession(headers=web.HEADERS, cookie_jar=jar, timeout=session_timeout) as session:
            # From the files.
            vldrs_ftchrs = ((fileapi.fetch_data_file(fileapi.FILE_NAMES[3]),
                             database.insert_gsm_table, GsmTable,),
                            (fileapi.fetch_data_file(fileapi.FILE_NAMES[7]),
                             database.insert_tank_table, TankTable,),
                            (fileapi.fetch_data_file(fileapi.FILE_NAMES[6]),
                             database.insert_sheet_table, SheetTable,),
                            (fileapi.fetch_data_file(fileapi.FILE_NAMES[0]),
                             database.insert_azs_table, AZSTable,),
                            (fileapi.fetch_data_file(fileapi.FILE_NAMES[2]),
                             database.insert_exchange_table, ExchangeTable,),
                            (fileapi.fetch_data_file(fileapi.FILE_NAMES[5]),
                             database.insert_remains_table, RemainsTable,))
            # From the endpoints
            # vldrs_ftchrs = ((web.fetch_table(web.webapi.URL_GSM_TABLE, session),
            #                  database.insert_gsm_table, GsmTable,),
            #                 (web.fetch_table(web.webapi.URL_TANK_TABLE, session),
            #                  database.insert_tank_table, TankTable,),
            #                 (web.fetch_table(web.webapi.URL_SHEET_TABLE, session),
            #                  database.insert_sheet_table, SheetTable,),
            #                 (web.fetch_table(web.webapi.URL_AZS_TABLE, session),
            #                  database.insert_azs_table, AZSTable,),
            #                 (web.fetch_table(web.webapi.URL_EXCHANGE_TABLE, session),
            #                  database.insert_exchange_table, ExchangeTable,),
            #                 (web.fetch_table(web.webapi.URL_REMAINS_TABLE, session),
            #                  database.insert_remains_table, RemainsTable,),)

            tasks = [asyncio.create_task(bot_table(session,
                                                   pool,
                                                   fetcher=vf[0],
                                                   insert_table=vf[1],
                                                   validator=vf[2])) for vf in vldrs_ftchrs]
            await asyncio.gather(*tasks)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    asyncio.run(main())
