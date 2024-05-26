import logging
import sys

import argparse

import aiohttp
import asyncio
import aiofiles

from yarl import URL
from pathlib import Path

import csv
import json

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'

if __name__ == '__main__':
    # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent

# Importing project packages.
import webapi

# Defining file names.
FILE_NAMES = ['dump_get_azs_table.data', 'dump_get_dailyrep_table.data',
              'dump_get_exchange_table.data', 'dump_get_gsm_table.data',
              'dump_get_oilrep_table.data', 'dump_get_remains_table.data',
              'dump_get_sheet_table.data', 'dump_get_tank_table.data', ]

# Defining field names for CSV format.
FILD_NAMES = {'dump_get_azs_table.data': ['uch', 'dt_giveout_a', 'storekeeper_name_a', 'given_litres_a', 'given_kg_a',
                                          'table_color', 'date_color', 'counter_azs_begin_day_a', 'counter_azs_end_day',
                                          'status', 'status_id', 'guid'],
              'dump_get_dailyrep_table.data': ['uch', 'dt_dailyreport', 'storekeeper_name_dr', 'photo_src', 'status',
                                               'table_color', 'date_color', 'status_id', 'guid'],
              'dump_get_exchange_table.data': ['uch', 'dt_change_tc', 'storekeeper_name_tc', 'tanker_in_tc',
                                               'table_color', 'date_color', 'tanker_out_tc', 'litres_tc', 'density_tc',
                                               'status', 'status_id', 'guid'],
              'dump_get_gsm_table.data': ['uch', 'user', 'dt_receiving', 'income_kg', 'table_color', 'date_color',
                                          'provider', 'contractor', 'venchile_gn', 'status', 'status_id', 'guid'],
              'dump_get_oilrep_table.data': ['uch', 'dt_oilreport', 'storekeeper_name_oil', 'photo_src', 'table_color',
                                             'date_color', 'status', 'status_id', 'guid'],
              'dump_get_remains_table.data': ['uch', 'dt_inspection_r', 'inspector_fio_r', 'section_number_r',
                                              'correction', 'remains_kg_r', 'fuel_mark_r', 'status', 'status_id',
                                              'guid'],
              'dump_get_sheet_table.data': ['date_s', 'storekeeper_name_s', 'shift_s', 'atz_tanker_sheet',
                                            'uch_s', 'given_litres_s', 'given_kg_s', 'status', 'status_id',
                                            'guid', 'date_color', 'table_color'],
              'dump_get_tank_table.data': ['dt_giveout_t', 'uch', 'venchile_gn_t', 'wp_dest_t', 'given_mass_t',
                                           'table_color', 'date_color', 'status', 'status_id', 'guid']}


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
        async for data_table in webapi.fetch_table(url, session, dt_beg=dt_beg, dt_end=dt_end):
            await f.write(data_table + '\n')
            # await f.write('\n')
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
    tasks = [asyncio.create_task(dumpdata_file(webapi.URL_ENDPOINT[url], session)) for url in urls]
    status_res = await asyncio.gather(*tasks)
    logging.info('Stop!')
    return status_res


async def fetch_data_file(name: str):
    """
    Fetch data from a dump file.
    """
    try:
        logging.info(f'Start({name})!')
        file_name = DATA_DIR / name
        if not file_name.exists():
            logging.error(f'The file({name}) does not exist!')
            return
        async with aiofiles.open(file_name, 'r') as f:
            async for line in f:
                yield line
    finally:
        logging.info(f'Stop({name})!')


async def save_data_csv_files(name: str):
    """
    Resave data in CSV format.
    """
    name_csv = DATA_DIR / (name.split('.')[0] + '.csv')
    logging.info(f'Start({name_csv})!')

    async with aiofiles.open(name_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FILD_NAMES[name])
        await writer.writeheader()
        async for raw_data in fetch_data_file(name):
            json_data = json.loads(raw_data)['ret']
            for dt in json_data:
                await writer.writerow(dt)
    logging.info(f'Stop({name_csv})!')


async def save_data_csv_main():
    """
    Starts asynchronous resave of data in CSV format.
    """
    logging.info('Start!')
    tasks = [asyncio.create_task(save_data_csv_files(fn)) for fn in FILE_NAMES]
    results = await asyncio.gather(*tasks)

    logging.info(f'Stop({results})!')


async def main(command: str):
    if command == 'dump':
        jar = aiohttp.CookieJar(unsafe=True)
        async with aiohttp.ClientSession(headers=webapi.HEADERS, cookie_jar=jar) as session:
            await webapi.login_user(session)
            await dumpdata_main(session)
    else:
        await save_data_csv_main()

    # await dumpdata_file(URL_GSM_TABLE, session) # , dt_beg="25.03.2024", dt_end="10.05.2024")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['dump', 'resave'],
                        help='1. "dump" the data to a file from the endpoint(url).\n2. "resave" of data in CSV forma.')
    args = parser.parse_args()

    logging.info(f'The program is running({args.command})!')
    asyncio.run(main(args.command))
    logging.info(f'The program has been stopped({args.command})!')
