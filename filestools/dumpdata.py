import logging
import sys

import argparse

import aiohttp
import asyncio
import aiofiles

from yarl import URL
from pathlib import Path

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
if not DATA_DIR.exists():
    DATA_DIR.mkdir()

if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent

# Importing project packages.
import web
import fileapi


async def dumpdata_file(url: str, session: aiohttp.ClientSession, *, dt_beg: str = '', dt_end: str = ''):
    """
    Dump the data to a file from the endpoint(url).
    """
    # print(url)
    endpoint_name = URL(url).path[1: len(URL(url).path) - 1]
    name = 'dump_' + endpoint_name + '.data'
    file_name = DATA_DIR / name
    logging.info(f'Start (endpoint={endpoint_name})!')

    async with aiofiles.open(file_name, 'w') as f:
        async for data_table in web.fetch_table(url, session, dt_beg=dt_beg, dt_end=dt_end):
            await f.write(data_table + '\n')

    logging.info(f'Stop (endpoint={endpoint_name})!')


async def dumpdata_main(session: aiohttp.ClientSession):
    logging.info('Start!')
    urls = ['URL_GSM_TABLE',
            'URL_TANK_TABLE',
            'URL_SHEET_TABLE',
            'URL_AZS_TABLE',
            'URL_REMAINS_TABLE',
            'URL_EXCHANGE_TABLE',
            'URL_DAILYREP_TABLE',
            'URL_OILREP_REP', ]
    tasks = [asyncio.create_task(dumpdata_file(web.URL_ENDPOINT[url], session)) for url in urls]
    status_res = await asyncio.gather(*tasks)
    logging.info('Stop!')
    return status_res


async def save_data_csv_main():
    """
    Starts asynchronous resave of data in CSV format.
    """
    logging.info('Start!')
    tasks = [asyncio.create_task(fileapi.save_data_csv_files(fn)) for fn in fileapi.FILE_NAMES]
    results = await asyncio.gather(*tasks)
    logging.info(f'Stop({results})!')


async def main(command: str):
    if command == 'dump':
        jar = aiohttp.CookieJar(unsafe=True)
        async with aiohttp.ClientSession(headers=web.HEADERS, cookie_jar=jar) as session:
            await web.login_user(session)
            await dumpdata_main(session)
    else:
        await save_data_csv_main()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['dump', 'resave'],
                        help='1. "dump" the data to a file from the endpoint(url).\n2. "resave" of data in CSV forma.')
    args = parser.parse_args()
    asyncio.run(main(args.command))
