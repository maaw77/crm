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

# Importing project packages.
from settings_management import Settings

# Defining sql commands for creating tables.
CREATE_SITES_TABLE = \
                    """
                    CREATE TABLE IF NOT EXISTS sites (
                            id SERIAL PRIMARY KEY,
                            name text
                    );"""

CREATE_OPERATORS_TABLE = \
                    """
                    CREATE TABLE IF NOT EXISTS operators (
                            id SERIAL PRIMARY KEY,
                            name text
                    );"""

CREATE_PROVIDERS_TABLE = \
                    """
                    CREATE TABLE IF NOT EXISTS providers (
                            id SERIAL PRIMARY KEY,
                            name text
                    );"""

CREATE_CONTRACTORS_TABLE = \
                    """
                    CREATE TABLE IF NOT EXISTS contractors (
                            id SERIAL PRIMARY KEY,
                            name text
                    );"""

CREATE_LICENSE_PLATES_TABLE = \
                    """
                    CREATE TABLE IF NOT EXISTS license_plates (
                            id SERIAL PRIMARY KEY,
                            name text
                    );"""

CREATE_STATUSES_TABLE = \
                    """
                    CREATE TABLE IF NOT EXISTS statuses (
                            id SERIAL PRIMARY KEY,
                            name text
                    );"""


CREATE_GSM_TABLE = \
                    """
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
                    );"""

CREATE_INDEX_GSM_TABLE = '''CREATE INDEX gsm_table_dt_receiving ON gsm_table (dt_receiving);'''


async def drop_tables(con: asyncpg.Connection):
    """
    Remove tables.
    """

    logging.info('Starting to delete database tables!')
    statements = ['DROP TABLE IF EXISTS sites CASCADE;', 'DROP TABLE IF EXISTS operators CASCADE;',
                  'DROP TABLE IF EXISTS providers CASCADE;', 'DROP TABLE IF EXISTS contractors CASCADE;',
                  'DROP TABLE IF EXISTS license_plates CASCADE;', 'DROP TABLE IF EXISTS statuses CASCADE;',
                  'DROP TABLE IF EXISTS gsm_table CASCADE;',]
    for statement in statements:
        status = await con.execute(statement)
        logging.info(f'Status of the last SQL command: {status} {statement.split()[2]}')
    logging.info('Stopping the deletion of database tables!')


async def create_tables(con: asyncpg.Connection):
    """
    Create tables.
    """
    logging.info('Starting creating database tables!')
    statements = [CREATE_SITES_TABLE, CREATE_OPERATORS_TABLE,
                  CREATE_PROVIDERS_TABLE, CREATE_CONTRACTORS_TABLE,
                  CREATE_LICENSE_PLATES_TABLE, CREATE_STATUSES_TABLE,
                  CREATE_GSM_TABLE, CREATE_INDEX_GSM_TABLE, ]
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
    if command == 'init':
        await create_tables(connection)
    elif command == 'delete':
        await drop_tables(connection)

    await connection.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['init', 'delete'],
                        help='1. "init" - creates tables in the database.\n2. "delete"tables in the database.')
    args = parser.parse_args()
    asyncio.run(main(args.command))
