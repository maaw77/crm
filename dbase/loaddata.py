import logging

import asyncio
import asyncpg
import aiohttp

from typing import Union, Type
from collections.abc import AsyncIterable, Callable

import sys
from pathlib import Path

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent

# Importing project packages.
from settings_management import Settings
from dbase.data_validators import (GsmTable, TankTable, SheetTable, AZSTable,
                                   ExchangeTable, RemainsTable,  deserial_valid)
import web
from filestools import fileapi
from dbase.database import (insert_gsm_table, insert_tank_table, insert_sheet_table,
                      insert_azs_table, insert_exchange_table, insert_remains_table)


async def loaddata_table(pool: asyncpg.Pool, dataflow: AsyncIterable, insert_table: Callable,
                         validator: Union[Type[GsmTable], Type[TankTable], Type[SheetTable],
                                          Type[AZSTable], Type[ExchangeTable], Type[RemainsTable]]) -> str:
    """
    Loads the contents of the "dataflow" into the database.
    """
    logging.info('Starting the data upload to the database!')
    async with pool.acquire() as conn:
        async for raw_data in dataflow:
            async for data in deserial_valid(raw_data, validator):
                await insert_table(conn, data)
    logging.info('Stopping data loading into the database!')
    return 'OK!'


async def main():
    """
    From the files.
    """
    con_par = Settings()  # Loading environment variables
    async with asyncpg.create_pool(host=con_par.HOST_DB,
                                   port=con_par.PORT_DB,
                                   user=con_par.POSTGRES_USER,
                                   database=con_par.POSTGRES_DB,
                                   password=con_par.POSTGRES_PASSWORD.get_secret_value()) as pool:

        data_loaders = [loaddata_table(pool,
                                       fileapi.fetch_data_file(fileapi.FILE_NAMES[3]),
                                       insert_gsm_table, GsmTable),
                        loaddata_table(pool,
                                       fileapi.fetch_data_file(fileapi.FILE_NAMES[7]),
                                       insert_tank_table, TankTable),
                        loaddata_table(pool,
                                       fileapi.fetch_data_file(fileapi.FILE_NAMES[6]),
                                       insert_sheet_table, SheetTable),
                        loaddata_table(pool,
                                       fileapi.fetch_data_file(fileapi.FILE_NAMES[0]),
                                       insert_azs_table, AZSTable),
                        loaddata_table(pool,
                                       fileapi.fetch_data_file(fileapi.FILE_NAMES[2]),
                                       insert_exchange_table, ExchangeTable),
                        loaddata_table(pool,
                                       fileapi.fetch_data_file(fileapi.FILE_NAMES[5]),
                                       insert_remains_table, RemainsTable),]
        tasks = [await asyncio.create_task(data_loader) for data_loader in data_loaders]
        logging.info(tasks)


# async def main():
#     """
#     From the endpoints.
#     """
#     con_par = Settings()  # Loading environment variables
#     async with asyncpg.create_pool(host=con_par.HOST_DB,
#                                    port=con_par.PORT_DB,
#                                    user=con_par.POSTGRES_USER,
#                                    database=con_par.POSTGRES_DB,
#                                    password=con_par.POSTGRES_PASSWORD.get_secret_value()) as pool:
#         jar = aiohttp.CookieJar(unsafe=True)
#         async with aiohttp.ClientSession(headers=web.HEADERS, cookie_jar=jar) as session:
#             data_loaders = (loaddata_table(pool, web.fetch_table(web.webapi.URL_GSM_TABLE, session),
#                                            insert_gsm_table, GsmTable),
#                             loaddata_table(pool, web.fetch_table(web.webapi.URL_TANK_TABLE, session),
#                                            insert_tank_table, TankTable),
#                             loaddata_table(pool, web.fetch_table(web.webapi.URL_SHEET_TABLE, session),
#                                            insert_sheet_table, SheetTable),
#                             loaddata_table(pool, web.fetch_table(web.webapi.URL_AZS_TABLE, session),
#                                            insert_azs_table, AZSTable),
#                             loaddata_table(pool, web.fetch_table(web.webapi.URL_EXCHANGE_TABLE, session),
#                                            insert_exchange_table, ExchangeTable),
#                             loaddata_table(pool, web.fetch_table(web.webapi.URL_REMAINS_TABLE, session),
#                                            insert_remains_table, RemainsTable),)
#             await web.login_user(session)
#             tasks = [await asyncio.create_task(data_loader) for data_loader in data_loaders]
#             logging.info(tasks)



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    asyncio.run(main())
