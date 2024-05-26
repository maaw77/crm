import logging

import asyncio
import asyncpg

from collections.abc import AsyncIterable
from typing import Callable

# import sys
# from pathlib import Path
# Defining the paths.
# BASE_DIR = Path(__file__).resolve().parent
# if __name__ == '__main__':
#     sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent

# Importing project packages.
from settings_management import Settings
from data_validators import GsmTable, deserial_valid
from fileapi import fetchdata
from database import insert_gsm_table


# async def loaddata_gsm_table(pool: asyncpg.Pool, dataflow: AsyncIterable):
#     """
#     Loads the contents of the " dataflow" into the database.
#     """
#     logging.info('Starting the data upload to the database!')
#     async with pool.acquire() as conn:
#         async for raw_data in dataflow:
#             async for data in deserial_valid(raw_data, GsmTable):
#                 await insert_gsm_table(conn, data)
#     logging.info('Stopping data loading into the database!')
#

async def loaddata_table(pool: asyncpg.Pool, dataflow: AsyncIterable, insert_table: Callable):
    """
    Loads the contents of the " dataflow" into the database.
    """
    logging.info('Starting the data upload to the database!')
    async with pool.acquire() as conn:
        async for raw_data in dataflow:
            async for data in deserial_valid(raw_data, GsmTable):
                await insert_table(conn, data)
    logging.info('Stopping data loading into the database!')


async def main():
    con_par = Settings()  # Loading environment variables
    async with asyncpg.create_pool(host=con_par.HOST_DB,
                                   port=con_par.PORT_DB,
                                   user=con_par.POSTGRES_USER,
                                   database=con_par.POSTGRES_DB,
                                   password=con_par.POSTGRES_PASSWORD.get_secret_value()) as pool:
        await loaddata_table(pool, fetchdata.fetch_data_file(fetchdata.FILE_NAMES[3]), insert_gsm_table)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    asyncio.run(main())
