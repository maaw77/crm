import logging

from pathlib import Path

import sys

import json
import csv

import aiofiles

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR/'data'


# Defining file names.
FILE_NAMES = ['dump_get_azs_table.data',  # Vidach iz TRK
              'dump_get_dailyrep_table.data',  # Sutochmye otchety
              'dump_get_exchange_table.data',  # Obmen megdu rezervuarami
              'dump_get_gsm_table.data',  # Priem
              'dump_get_oilrep_table.data',  # Vidacha masel
              'dump_get_remains_table.data',   # Snyatie ostatkov
              'dump_get_sheet_table.data',  # Vidacha iz ATZ
              'dump_get_tank_table.data',]   # Vidacha v ATZ

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


async def fetch_data_file(name: str):
    """
    Fetching data from a file.
    :param name: File name.
    """
    try:
        logging.info(f'Start({name})!')
        file_name = DATA_DIR/name
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
    :param name: File name.
    """
    name_csv = DATA_DIR/(name.split('.')[0]+'.csv')
    logging.info(f'Start({name_csv})!')

    async with aiofiles.open(name_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FILD_NAMES[name])
        await writer.writeheader()
        async for raw_data in fetch_data_file(name):
            json_data = json.loads(raw_data)['ret']
            for dt in json_data:
                await writer.writerow(dt)
    logging.info(f'Stop({name_csv})!')

