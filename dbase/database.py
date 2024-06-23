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

SELECT_GSM_TABLE = \
                    '''
                    SELECT gsm_table.dt_receiving,
                           gsm_table.dt_crch,
                           gsm_table.been_changed,
                           sites.name AS site,
                           gsm_table.income_kg,
                           operators.name AS operator,
                           providers.name AS provider,
                           contractors.name AS contractor,
                           license_plates.name AS license_plate,
                           statuses.name AS status
                           FROM gsm_table 
                           JOIN sites ON gsm_table.site_id = sites.id
                           JOIN operators ON gsm_table.operator_id = operators.id
                           JOIN providers ON gsm_table.provider_id = providers.id
                           JOIN contractors ON gsm_table.contractor_id = contractors.id
                           JOIN license_plates ON gsm_table.license_plate_id = license_plates.id
                           JOIN statuses ON gsm_table.status_id = statuses.id
                           WHERE gsm_table.dt_receiving >= $1
                                 and gsm_table.dt_receiving <= $2
                           ORDER BY gsm_table.dt_receiving;
                    '''


SELECT_TANK_TABLE = \
                    '''
                    SELECT tank_table.dt_giveout,
                           tank_table.dt_crch,
                           tank_table.been_changed,
                           sites.name AS site,
                           onboard_nums.name AS onboard_num,
                           dest_sites.name AS dest_site,
                           tank_table.given_kg,
                           statuses.name AS status
                           FROM tank_table
                           JOIN sites ON tank_table.site_id = sites.id
                           JOIN onboard_nums ON tank_table.onboard_num_id = onboard_nums.id
                           JOIN sites dest_sites ON tank_table.dest_site_id = dest_sites.id
                           JOIN statuses ON tank_table.status_id = statuses.id
                           WHERE tank_table.dt_giveout >= $1
                                 and tank_table.dt_giveout <= $2
                           ORDER BY tank_table.dt_giveout;
                    '''

SELECT_SHEET_TABLE = \
                    '''
                    SELECT sheet_table.dt_giveout,
                           sheet_table.dt_crch,
                           sheet_table.been_changed,
                           sites.name AS site,
                           atzs.name AS atz,
                           give_sites.name AS give_site,
                           sheet_table.given_litres,
                           sheet_table.given_kg,
                           statuses.name AS status
                           FROM sheet_table
                           JOIN sites ON sheet_table.site_id = sites.id
                           JOIN atzs ON sheet_table.atz_id = atzs.id
                           JOIN sites give_sites ON sheet_table.give_site_id = give_sites.id
                           JOIN statuses ON sheet_table.status_id = statuses.id
                           WHERE sheet_table.dt_giveout >= $1
                                 and sheet_table.dt_giveout <= $2
                           ORDER BY sheet_table.dt_giveout;
                    '''

SELECT_AZS_TABLE = \
                    '''
                    SELECT azs_table.dt_giveout,
                           azs_table.dt_crch,
                           azs_table.been_changed,
                           sites.name AS site,
                           storekeepers.name AS storekeeper,
                           azs_table.counter_azs_bd,
                           azs_table.counter_azs_ed,
                           azs_table.given_litres,
                           azs_table.given_kg,
                           statuses.name AS status
                           FROM azs_table
                           JOIN sites ON azs_table.site_id = sites.id
                           JOIN storekeepers ON azs_table.storekeeper_id = storekeepers.id
                           JOIN statuses ON azs_table.status_id = statuses.id
                           WHERE azs_table.dt_giveout >= $1
                                 and azs_table.dt_giveout <= $2
                           ORDER BY azs_table.dt_giveout;
                    '''

SELECT_EXCHANGE_TABLE = \
                    '''
                    SELECT exchange_table.dt_change,
                           exchange_table.dt_crch,
                           exchange_table.been_changed,
                           sites.name AS site,
                           operators.name AS operator,
                           tin.name AS tanker_in,
                           tout.name AS tanker_out,
                           exchange_table.litres,
                           statuses.name AS status 
                           FROM exchange_table
                           JOIN sites ON exchange_table.site_id = sites.id
                           JOIN operators ON exchange_table.operator_id = operators.id
                           JOIN tankers tin ON exchange_table.tanker_in_id = tin.id
                           JOIN tankers tout ON exchange_table.tanker_out_id = tout.id
                           JOIN statuses ON exchange_table.status_id = statuses.id
                           WHERE exchange_table.dt_change >= $1
                                 and exchange_table.dt_change <= $2
                           ORDER BY exchange_table.dt_change;
                    '''


SELECT_REMAINS_TABLE = \
                    '''
                    SELECT remains_table.dt_inspection,
                           sites.name AS site,
                           inspectors.name AS inspector,
                           tankers.name AS tanker_num,
                           remains_table.remains_kg,
                           fuel_marks.name AS fuel_mark,
                           statuses.name AS status 
                           FROM remains_table
                           JOIN sites ON remains_table.site_id = sites.id
                           JOIN inspectors ON remains_table.inspector_id = inspectors.id
                           JOIN tankers ON remains_table.tanker_num_id = tankers.id
                           JOIN fuel_marks ON remains_table.fuel_mark_id = fuel_marks.id
                           JOIN statuses ON remains_table.status_id = statuses.id
                           WHERE remains_table.dt_inspection >= $1
                                 and remains_table.dt_inspection <= $2
                           ORDER BY remains_table.dt_inspection;
                    '''


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
            dest_site_id: int = await get_id_or_create(conn, 'sites', in_data.dest_site)
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
