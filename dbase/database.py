import logging

import asyncpg

from datetime import datetime

import sys
from pathlib import Path

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.paren

# Importing project's packages.
from dbase.data_validators import (GsmTable, TankTable, SheetTable, AZSTable, ExchangeTable, RemainsTable)


async def get_id_or_create(conn: asyncpg.Connection, nametable: str, val_record: str) -> int:
    """
    Returns the ID if the record exists, otherwise creates the record and returns its ID.
    """
    # statement_1 = 'SELECT id FROM {} WHERE name = $1;'.format(nametable)
    # statement_2 = 'INSERT INTO {} VALUES (DEFAULT, $1) RETURNING id;'.format(nametable)
    statement_1 = f'SELECT id FROM {nametable} WHERE name = $1;'
    statement_2 = f'INSERT INTO {nametable} VALUES (DEFAULT, $1) RETURNING id;'
    record: asyncpg.Record = await conn.fetchrow(statement_1, val_record)
    if record is not None:
        return record['id']
    record = await conn.fetchrow(statement_2, val_record)
    return record['id']


async def insert_gsm_table(conn: asyncpg.Connection, in_data: GsmTable) -> int:
    """
    Inserts data into the gsm ("Priem") table.
    """
    statement_1 = 'SELECT id  FROM gsm_table WHERE guid=$1;'
    statement_2 = '''INSERT INTO gsm_table (id, 
                                            dt_receiving,
                                            dt_crch,
                                            income_kg,
                                            been_changed,
                                            db_data_creation,
                                            site_id,
                                            operator_id,
                                            provider_id,
                                            contractor_id,
                                            license_plate_id,
                                            status_id,
                                            guid) 
                                            VALUES (DEFAULT, $1, $2, $3, $4,  $5, $6, $7, $8,  $9, $10, $11, $12)
                                            RETURNING id;'''

    async with conn.transaction():
        record: asyncpg.Record = await conn.fetchrow(statement_1, in_data.guid)
        if record is None:
            site_id: int = await get_id_or_create(conn, 'sites', in_data.site)
            operator_id: int = await get_id_or_create(conn, 'operators', in_data.operator)
            provider_id: int = await get_id_or_create(conn, 'providers', in_data.provider)
            contractor_id: int = await get_id_or_create(conn, 'contractors', in_data.contractor)
            license_plate_id: int = await get_id_or_create(conn, 'license_plates', in_data.license_plate)
            status_id: int = await get_id_or_create(conn, 'statuses', in_data.status)
            record: asyncpg.Record = await conn.fetchrow(statement_2, in_data.dt_receiving,
                                                                            in_data.date_crch,
                                                                            in_data.income_kg,
                                                                            in_data.been_changed,
                                                                            datetime.now(),
                                                                            site_id,
                                                                            operator_id,
                                                                            provider_id,
                                                                            contractor_id,
                                                                            license_plate_id,
                                                                            status_id,
                                                                            in_data.guid)

            logging.info(f"The guid (gsm table) = {in_data.guid} with ID = {record['id']}  was inserted.")
    return record['id']


async def insert_tank_table(conn: asyncpg.Connection, in_data: TankTable) -> int:
    """
    Inserts data into the tank("Vidacha v ATZ") table.
    """
    statement_1 = 'SELECT id  FROM tank_table WHERE guid=$1;'
    statement_2 = '''INSERT INTO tank_table (id, 
                                             dt_giveout,
                                             dt_crch,
                                             given_kg,
                                             been_changed,
                                             db_data_creation,
                                             site_id,
                                             onboard_num_id,
                                             dest_site_id,
                                             status_id,
                                             guid) 
                                             VALUES (DEFAULT, $1, $2, $3, $4,  $5, $6, $7, $8,  $9, $10)
                                             RETURNING id;'''

    async with conn.transaction():
        record: asyncpg.Record = await conn.fetchrow(statement_1, in_data.guid)
        if record is None:
            site_id: int = await get_id_or_create(conn, 'sites', in_data.site)
            onboard_num_id: int = await get_id_or_create(conn, 'onboard_nums', in_data.onboard_num)
            dest_site_id: int = await get_id_or_create(conn, 'dest_sites', in_data.dest_site)
            status_id: int = await get_id_or_create(conn, 'statuses', in_data.status)
            record: asyncpg.Record = await conn.fetchrow(statement_2, in_data.dt_giveout,
                                                                            in_data.date_crch,
                                                                            in_data.given_kg,
                                                                            in_data.been_changed,
                                                                            datetime.now(),
                                                                            site_id,
                                                                            onboard_num_id,
                                                                            dest_site_id,
                                                                            status_id,
                                                                            in_data.guid)

            logging.info(f"The guid (tank table) = {in_data.guid} with ID = {record['id']}  was inserted.")
    return record['id']


async def insert_sheet_table(conn: asyncpg.Connection, in_data: SheetTable) -> int:
    """
    Inserts data into the sheet ("Vidacha  iz ATZ") table.
    """
    statement_1 = 'SELECT id  FROM sheet_table WHERE guid=$1;'
    statement_2 = '''INSERT INTO sheet_table (id, 
                                              dt_giveout,
                                              dt_crch,
                                              given_litres,
                                              given_kg,
                                              been_changed,
                                              db_data_creation,
                                              site_id,
                                              atz_id,
                                              give_site_id,
                                              status_id,
                                              guid) 
                                              VALUES (DEFAULT, $1, $2, $3, $4,  $5, $6, $7, $8,  $9, $10, $11)
                                              RETURNING id;'''

    async with conn.transaction():
        record: asyncpg.Record = await conn.fetchrow(statement_1, in_data.guid)
        if record is None:
            site_id: int = await get_id_or_create(conn, 'sites', in_data.site)
            give_site_id: int = await get_id_or_create(conn, 'sites', in_data.give_site)
            atz_id: int = await get_id_or_create(conn, 'atzs', in_data.atz)
            status_id: int = await get_id_or_create(conn, 'statuses', in_data.status)
            record: asyncpg.Record = await conn.fetchrow(statement_2, in_data.dt_giveout,
                                                                            in_data.date_crch,
                                                                            in_data.given_litres,
                                                                            in_data.given_kg,
                                                                            in_data.been_changed,
                                                                            datetime.now(),
                                                                            site_id,
                                                                            atz_id,
                                                                            give_site_id,
                                                                            status_id,
                                                                            in_data.guid)

            logging.info(f"The guid (sheet table) = {in_data.guid} with ID = {record['id']}  was inserted.")
    return record['id']


async def insert_azs_table(conn: asyncpg.Connection, in_data: AZSTable) -> int:
    """
    Inserts data into the azs("Vidacha  iz TRK") table.
    """
    statement_1 = 'SELECT id  FROM azs_table WHERE guid=$1;'
    statement_2 = '''INSERT INTO azs_table (id, 
                                              dt_giveout,
                                              dt_crch,
                                              counter_azs_bd,
                                              counter_azs_ed,
                                              given_litres,
                                              given_kg,
                                              been_changed,
                                              db_data_creation,
                                              site_id,
                                              storekeeper_id,
                                              status_id,
                                              guid) 
                                              VALUES (DEFAULT, $1, $2, $3, $4,  $5, $6, $7, $8,  $9, $10, $11, $12)
                                              RETURNING id;'''

    async with conn.transaction():
        record: asyncpg.Record = await conn.fetchrow(statement_1, in_data.guid)
        if record is None:
            site_id: int = await get_id_or_create(conn, 'sites', in_data.site)
            storekeeper_id: int = await get_id_or_create(conn, 'storekeepers', in_data.storekeeper_name)
            status_id: int = await get_id_or_create(conn, 'statuses', in_data.status)
            record: asyncpg.Record = await conn.fetchrow(statement_2, in_data.dt_giveout,
                                                                            in_data.date_crch,
                                                                            in_data.counter_azs_bd,
                                                                            in_data.counter_azs_ed,
                                                                            in_data.given_litres,
                                                                            in_data.given_kg,
                                                                            in_data.been_changed,
                                                                            datetime.now(),
                                                                            site_id,
                                                                            storekeeper_id,
                                                                            status_id,
                                                                            in_data.guid)

            logging.info(f"The guid (sheet table) = {in_data.guid} with ID = {record['id']}  was inserted.")
    return record['id']


async def insert_exchange_table(conn: asyncpg.Connection, in_data: ExchangeTable) -> int:
    """
    Inserts data into the exchange("Obmen megdu rezervuarami") table.
    """
    statement_1 = 'SELECT id  FROM exchange_table WHERE guid=$1;'
    statement_2 = '''INSERT INTO exchange_table (id, 
                                                 dt_change,
                                                 dt_crch,
                                                 litres,
                                                 density,
                                                 been_changed,
                                                 db_data_creation,
                                                 site_id,
                                                 operator_id,
                                                 tanker_in_id,
                                                 tanker_out_id,
                                                 status_id,
                                                 guid) 
                                                 VALUES (DEFAULT, $1, $2, $3, $4,  $5, $6, $7, $8,  $9, $10, $11, $12)
                                                 RETURNING id;'''

    async with conn.transaction():
        record: asyncpg.Record = await conn.fetchrow(statement_1, in_data.guid)
        if record is None:
            site_id: int = await get_id_or_create(conn, 'sites', in_data.site)
            operator_id = await get_id_or_create(conn, 'operators', in_data.operator)
            tanker_in_id: int = await get_id_or_create(conn, 'tankers', in_data.tanker_in)
            tanker_out_id: int = await get_id_or_create(conn, 'tankers', in_data.tanker_out)
            status_id: int = await get_id_or_create(conn, 'statuses', in_data.status)
            record: asyncpg.Record = await conn.fetchrow(statement_2, in_data.dt_change,
                                                                            in_data.date_crch,
                                                                            in_data.litres,
                                                                            in_data.density,
                                                                            in_data.been_changed,
                                                                            datetime.now(),
                                                                            site_id,
                                                                            operator_id,
                                                                            tanker_in_id,
                                                                            tanker_out_id,
                                                                            status_id,
                                                                            in_data.guid)

            logging.info(f"The guid (sheet table) = {in_data.guid} with ID = {record['id']}  was inserted.")
    return record['id']


async def insert_remains_table(conn: asyncpg.Connection, in_data: RemainsTable) -> int:
    """
    Inserts data into the remains ("Snyatie ostatkov") table.
    """
    statement_1 = 'SELECT id  FROM remains_table WHERE guid=$1;'
    statement_2 = '''INSERT INTO remains_table (id, 
                                                dt_inspection,
                                                remains_kg,
                                                db_data_creation,
                                                site_id,
                                                inspector_id,
                                                tanker_num_id,
                                                fuel_mark_id,  
                                                status_id,
                                                guid) 
                                                VALUES (DEFAULT, $1, $2, $3, $4,  $5, $6, $7, $8,  $9)
                                              RETURNING id;'''

    async with conn.transaction():
        record: asyncpg.Record = await conn.fetchrow(statement_1, in_data.guid)
        if record is None:
            site_id: int = await get_id_or_create(conn, 'sites', in_data.site)
            inspector_id = await get_id_or_create(conn, 'inspectors', in_data.inspector)
            tanker_num_id: int = await get_id_or_create(conn, 'tankers', in_data.tanker_num)
            fuel_mark_id: int = await get_id_or_create(conn, 'fuel_marks', in_data.fuel_mark)
            status_id: int = await get_id_or_create(conn, 'statuses', in_data.status)
            record: asyncpg.Record = await conn.fetchrow(statement_2, in_data.dt_inspection,
                                                                            in_data.remains_kg,
                                                                            datetime.now(),
                                                                            site_id,
                                                                            inspector_id,
                                                                            tanker_num_id,
                                                                            fuel_mark_id,
                                                                            status_id,
                                                                            in_data.guid)

            logging.info(f"The guid (sheet table) = {in_data.guid} with ID = {record['id']}  was inserted.")
    return record['id']
