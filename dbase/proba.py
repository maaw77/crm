import logging

import asyncio
import asyncpg

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

async def get_id_or_create(conn: asyncpg.Connection, nametable: str, val_record: str):
    """
    Returns the ID if the record exists, otherwise creates the record and returns its ID.
    """
    # statement_1 = 'SELECT id FROM {} WHERE name = $1;'.format(nametable)
    # statement_2 = 'INSERT INTO {} VALUES (DEFAULT, $1) RETURNING id;'.format(nametable)
    statement_1 = f'SELECT id FROM {nametable} WHERE name = $1;'
    statement_2 = f'INSERT INTO {nametable} VALUES (DEFAULT, $1) RETURNING id;'
    record: asyncpg.Record = await conn.fetch(statement_1, val_record)
    if record is not None:
        return record
    record = await conn.fetch(statement_2, val_record)
    return record

async def loaddata_table(pool: asyncpg.Pool):
    """
    Loads the contents of the "dataflow" into the database.
    """
    async with pool.acquire() as conn:
        result = await get_id_or_create(conn,'sites', 'АНДАТ')
        print(result)


async def main():
    """
    From the file.
    """
    con_par = Settings()  # Loading environment variables
    async with asyncpg.create_pool(host=con_par.HOST_DB,
                                   port=con_par.PORT_DB,
                                   user=con_par.POSTGRES_USER,
                                   database=con_par.POSTGRES_DB,
                                   password=con_par.POSTGRES_PASSWORD.get_secret_value()) as pool:
        await loaddata_table(pool)





if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    asyncio.run(main())
