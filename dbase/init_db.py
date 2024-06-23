import argparse

import logging

import asyncio
import asyncpg

import sys
from pathlib import Path
# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent

# Importing project's packages.
from settings_management import Settings

# Defining sql commands for creating tables.
CREATE_SITES_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS sites (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

# CREATE_DEST_SITES_TABLE = \
#                     '''
#                     CREATE TABLE IF NOT EXISTS dest_sites (
#                             id SERIAL PRIMARY KEY,
#                             name text
#                     );'''

CREATE_OPERATORS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS operators (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_PROVIDERS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS providers (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_CONTRACTORS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS contractors (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_LICENSE_PLATES_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS license_plates (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_STATUSES_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS statuses (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_ONBOARD_NUMS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS onboard_nums (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_ATZS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS atzs (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_STOREKEEPERS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS storekeepers (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_TANKERS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS tankers (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_INSPECTORS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS inspectors (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_FUEL_MARKS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS fuel_marks (
                            id SERIAL PRIMARY KEY,
                            name text
                    );'''

CREATE_GSM_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS gsm_table (
                            id SERIAL PRIMARY KEY,
                            dt_receiving date,
                            dt_crch date,
                            income_kg real,
                            been_changed boolean,
                            db_data_creation timestamp,
                            site_id integer REFERENCES sites ON DELETE CASCADE,
                            operator_id integer REFERENCES operators ON DELETE CASCADE,
                            provider_id integer REFERENCES providers ON DELETE CASCADE,
                            contractor_id integer REFERENCES contractors ON DELETE CASCADE,
                            license_plate_id integer REFERENCES license_plates ON DELETE CASCADE,
                            status_id integer REFERENCES  statuses ON DELETE CASCADE,           
                            guid text    
                    );'''

CREATE_INDEX_GSM_TABLE_1 = '''CREATE INDEX IF NOT EXISTS gsm_table_dt_receiving ON gsm_table (dt_receiving);'''
CREATE_INDEX_GSM_TABLE_2 = '''CREATE INDEX IF NOT EXISTS gsm_table_guid ON gsm_table (guid);'''


# CREATE_TANK_TABLE = \
#                     '''
#                     CREATE TABLE IF NOT EXISTS tank_table (
#                             id SERIAL PRIMARY KEY,
#                             dt_giveout date,
#                             dt_crch date,
#                             given_kg real,
#                             been_changed boolean,
#                             db_data_creation timestamp,
#                             site_id integer REFERENCES sites ON DELETE CASCADE,
#                             onboard_num_id integer REFERENCES  onboard_nums ON DELETE CASCADE,
#                             dest_site_id integer REFERENCES dest_sites ON DELETE CASCADE,
#                             status_id integer REFERENCES  statuses ON DELETE CASCADE,
#                             guid text
#                       );'''

CREATE_TANK_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS tank_table (
                            id SERIAL PRIMARY KEY,
                            dt_giveout date,
                            dt_crch date,
                            given_kg real,
                            been_changed boolean,
                            db_data_creation timestamp,
                            site_id integer REFERENCES sites ON DELETE CASCADE,
                            onboard_num_id integer REFERENCES  onboard_nums ON DELETE CASCADE,
                            dest_site_id integer REFERENCES sites ON DELETE CASCADE, 
                            status_id integer REFERENCES  statuses ON DELETE CASCADE,           
                            guid text                                                         
                      );'''

CREATE_INDEX_TANK_TABLE_1 = 'CREATE INDEX IF NOT EXISTS tank_table_dt_giveout ON tank_table (dt_giveout);'
CREATE_INDEX_TANK_TABLE_2 = 'CREATE INDEX IF NOT EXISTS tank_table_guid ON tank_table (guid);'

CREATE_SHEET_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS sheet_table (
                            id SERIAL PRIMARY KEY,
                            dt_giveout date,
                            dt_crch date,
                            given_litres real,
                            given_kg real,
                            been_changed boolean,
                            db_data_creation timestamp,
                            site_id integer REFERENCES sites ON DELETE CASCADE,
                            atz_id integer REFERENCES atzs ON DELETE CASCADE,
                            give_site_id integer REFERENCES sites ON DELETE CASCADE,
                            status_id integer REFERENCES  statuses ON DELETE CASCADE,           
                            guid text                                                         
                      );'''
CREATE_INDEX_SHEET_TABLE_1 = 'CREATE INDEX IF NOT EXISTS sheet_table_dt_giveout ON sheet_table (dt_giveout);'
CREATE_INDEX_SHEET_TABLE_2 = 'CREATE INDEX IF NOT EXISTS sheet_table_guid ON sheet_table (guid);'


CREATE_AZS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS azs_table (
                            id SERIAL PRIMARY KEY,
                            dt_giveout date,
                            dt_crch date,
                            counter_azs_bd real,
                            counter_azs_ed real,
                            given_litres real,
                            given_kg real,
                            been_changed boolean,
                            db_data_creation timestamp,                            
                            site_id integer REFERENCES sites ON DELETE CASCADE,
                            storekeeper_id integer REFERENCES storekeepers ON DELETE CASCADE,
                            status_id integer REFERENCES  statuses ON DELETE CASCADE,           
                            guid text                                                         
                      );'''

CREATE_INDEX_AZS_TABLE_1 = 'CREATE INDEX IF NOT EXISTS asz_table_dt_giveout ON azs_table (dt_giveout);'
CREATE_INDEX_AZS_TABLE_2 = 'CREATE INDEX IF NOT EXISTS asz_table_guid ON azs_table (guid);'


CREATE_EXCHANGE_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS exchange_table (
                            id SERIAL PRIMARY KEY,
                            dt_change date,
                            dt_crch date,
                            litres real,
                            density real,
                            site_id integer REFERENCES sites ON DELETE CASCADE,
                            operator_id integer REFERENCES operators ON DELETE CASCADE,
                            tanker_in_id integer REFERENCES tankers ON DELETE CASCADE,
                            tanker_out_id integer REFERENCES tankers ON DELETE CASCADE,
                            been_changed boolean,
                            db_data_creation timestamp,                            
                            status_id integer REFERENCES  statuses ON DELETE CASCADE,           
                            guid text                                                         
                      );'''

CREATE_INDEX_EXCHANGE_TABLE_1 = 'CREATE INDEX IF NOT EXISTS exchange_table_dt_change ON exchange_table (dt_change);'
CREATE_INDEX_EXCHANGE_TABLE_2 = 'CREATE INDEX IF NOT EXISTS exchange_table_guid ON exchange_table (guid);'

CREATE_REMAINS_TABLE = \
                    '''
                    CREATE TABLE IF NOT EXISTS remains_table (
                            id SERIAL PRIMARY KEY,
                            dt_inspection date,
                            remains_kg real,
                            site_id integer REFERENCES sites ON DELETE CASCADE,
                            inspector_id integer REFERENCES inspectors ON DELETE CASCADE,
                            tanker_num_id integer REFERENCES tankers ON DELETE CASCADE,
                            fuel_mark_id integer REFERENCES fuel_marks ON DELETE CASCADE,
                            db_data_creation timestamp,                            
                            status_id integer REFERENCES  statuses ON DELETE CASCADE,           
                            guid text                                                         
                      );'''


CREATE_INDEX_REMAINS_TABLE_1 = \
                            'CREATE INDEX IF NOT EXISTS remains_table_dt_inspection ON remains_table (dt_inspection);'
CREATE_INDEX_REMAINS_TABLE_2 = 'CREATE INDEX IF NOT EXISTS remains_table_guid ON remains_table (guid);'


async def drop_tables(con: asyncpg.Connection):
    """
    Remove tables.
    """
    logging.info('Starting to delete database tables!')
    # statements = ['DROP TABLE IF EXISTS sites CASCADE;', 'DROP TABLE IF EXISTS operators CASCADE;',
    #               'DROP TABLE IF EXISTS providers CASCADE;', 'DROP TABLE IF EXISTS contractors CASCADE;',
    #               'DROP TABLE IF EXISTS license_plates CASCADE;', 'DROP TABLE IF EXISTS statuses CASCADE;',
    #               'DROP TABLE IF EXISTS gsm_table CASCADE;', 'DROP TABLE IF EXISTS dest_sites CASCADE;',
    #               'DROP TABLE IF EXISTS onboard_nums CASCADE;', 'DROP TABLE IF EXISTS tank_table  CASCADE;',
    #               'DROP TABLE IF EXISTS atzs CASCADE;', 'DROP TABLE IF EXISTS sheet_table CASCADE;',
    #               'DROP TABLE IF EXISTS storekeepers CASCADE;', 'DROP TABLE IF EXISTS azs_table CASCADE;',
    #               'DROP TABLE IF EXISTS tankers CASCADE;', 'DROP TABLE IF EXISTS exchange_table CASCADE;',
    #               'DROP TABLE IF EXISTS inspectors CASCADE;', 'DROP TABLE IF EXISTS fuel_marks CASCADE;',
    #               'DROP TABLE IF EXISTS remains_table CASCADE;',]

    statements = ['DROP TABLE IF EXISTS sites CASCADE;', 'DROP TABLE IF EXISTS operators CASCADE;',
                  'DROP TABLE IF EXISTS providers CASCADE;', 'DROP TABLE IF EXISTS contractors CASCADE;',
                  'DROP TABLE IF EXISTS license_plates CASCADE;', 'DROP TABLE IF EXISTS statuses CASCADE;',
                  'DROP TABLE IF EXISTS gsm_table CASCADE;',
                  'DROP TABLE IF EXISTS onboard_nums CASCADE;', 'DROP TABLE IF EXISTS tank_table  CASCADE;',
                  'DROP TABLE IF EXISTS atzs CASCADE;', 'DROP TABLE IF EXISTS sheet_table CASCADE;',
                  'DROP TABLE IF EXISTS storekeepers CASCADE;', 'DROP TABLE IF EXISTS azs_table CASCADE;',
                  'DROP TABLE IF EXISTS tankers CASCADE;', 'DROP TABLE IF EXISTS exchange_table CASCADE;',
                  'DROP TABLE IF EXISTS inspectors CASCADE;', 'DROP TABLE IF EXISTS fuel_marks CASCADE;',
                  'DROP TABLE IF EXISTS remains_table CASCADE;',]
    for statement in statements:
        status = await con.execute(statement)
        logging.info(f'Status of the last SQL command: {status} {statement.split()[4]};')
    logging.info('Stopping the deletion of database tables!')


async def create_tables(con: asyncpg.Connection):
    """
    Create tables.
    """
    logging.info('Starting creating database tables!')
    # statements = [CREATE_SITES_TABLE, CREATE_OPERATORS_TABLE,
    #               CREATE_PROVIDERS_TABLE, CREATE_CONTRACTORS_TABLE,
    #               CREATE_LICENSE_PLATES_TABLE, CREATE_STATUSES_TABLE,
    #               CREATE_GSM_TABLE, CREATE_INDEX_GSM_TABLE_1, CREATE_INDEX_GSM_TABLE_2,
    #               CREATE_DEST_SITES_TABLE, CREATE_ONBOARD_NUMS_TABLE, CREATE_TANK_TABLE,
    #               CREATE_INDEX_TANK_TABLE_1, CREATE_INDEX_TANK_TABLE_2, CREATE_ATZS_TABLE,
    #               CREATE_SHEET_TABLE, CREATE_INDEX_SHEET_TABLE_1, CREATE_INDEX_SHEET_TABLE_2,
    #               CREATE_STOREKEEPERS_TABLE, CREATE_AZS_TABLE, CREATE_INDEX_AZS_TABLE_1,
    #               CREATE_INDEX_AZS_TABLE_2, CREATE_TANKERS_TABLE, CREATE_EXCHANGE_TABLE,
    #               CREATE_INDEX_EXCHANGE_TABLE_1, CREATE_INDEX_EXCHANGE_TABLE_2,
    #               CREATE_INSPECTORS_TABLE, CREATE_FUEL_MARKS_TABLE, CREATE_REMAINS_TABLE,
    #               CREATE_INDEX_REMAINS_TABLE_1, CREATE_INDEX_REMAINS_TABLE_2]

    statements = [CREATE_SITES_TABLE, CREATE_OPERATORS_TABLE,
                  CREATE_PROVIDERS_TABLE, CREATE_CONTRACTORS_TABLE,
                  CREATE_LICENSE_PLATES_TABLE, CREATE_STATUSES_TABLE,
                  CREATE_GSM_TABLE, CREATE_INDEX_GSM_TABLE_1, CREATE_INDEX_GSM_TABLE_2,
                  CREATE_ONBOARD_NUMS_TABLE, CREATE_TANK_TABLE,
                  CREATE_INDEX_TANK_TABLE_1, CREATE_INDEX_TANK_TABLE_2, CREATE_ATZS_TABLE,
                  CREATE_SHEET_TABLE, CREATE_INDEX_SHEET_TABLE_1, CREATE_INDEX_SHEET_TABLE_2,
                  CREATE_STOREKEEPERS_TABLE, CREATE_AZS_TABLE, CREATE_INDEX_AZS_TABLE_1,
                  CREATE_INDEX_AZS_TABLE_2, CREATE_TANKERS_TABLE, CREATE_EXCHANGE_TABLE,
                  CREATE_INDEX_EXCHANGE_TABLE_1, CREATE_INDEX_EXCHANGE_TABLE_2,
                  CREATE_INSPECTORS_TABLE, CREATE_FUEL_MARKS_TABLE, CREATE_REMAINS_TABLE,
                  CREATE_INDEX_REMAINS_TABLE_1, CREATE_INDEX_REMAINS_TABLE_2]

    for statement in statements:
        status = await con.execute(statement)
        logging.info(f'Status of the last SQL command: {status} {statement.split()[5]};')
    logging.info('Stopping the creation of database tables!')


async def main(command: str):
    con_par = Settings()  # Loading environment variables
    connection: asyncpg.Connection = await asyncpg.connect(host=con_par.HOST_DB,
                                                           port=con_par.PORT_DB,
                                                           user=con_par.POSTGRES_USER,
                                                           database=con_par.POSTGRES_DB,
                                                           password=con_par.POSTGRES_PASSWORD.get_secret_value())

    # version = connection.get_server_version()
    # print(version)
    match command:
        case 'init':
            await create_tables(connection)
        case 'delete':
            await drop_tables(connection)
        case _:
            print('Unknown command!')
    # if command == 'init':
    #     await create_tables(connection)

    #     await drop_tables(connection)

    await connection.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['init', 'delete'],
                        help='1. "init" - creates tables in the database.\n2. "delete" tables in the database.')
    args = parser.parse_args()
    asyncio.run(main(args.command))
