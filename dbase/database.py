import logging

import asyncpg

from datetime import datetime

# Importing project packages.
from data_validators import GsmTable


async def get_id_or_create(conn: asyncpg.Connection, nametable: str, val_record: str) -> int:
    """
    Returns the ID if the record exists, otherwise creates the record and returns its ID.
    """
    statement_1 = "SELECT id FROM {} WHERE name = $1;".format(nametable)
    statement_2 = "INSERT INTO {} VALUES (DEFAULT, $1) RETURNING id;".format(nametable)
    record: asyncpg.Record = await conn.fetchrow(statement_1, val_record)
    if record is not None:
        return record['id']
    record = await conn.fetchrow(statement_2, val_record)
    return record['id']


async def insert_gsm_table(conn: asyncpg.Connection, in_data: GsmTable) -> int:
    """
    Inserts data into the gsm ("Priem") table.
    !!Make a Transaction!!!
    """
    statement = '''INSERT INTO gsm_table (id, 
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
                                            RETURNING id'''

    async with conn.transaction():
        site_id: int = await get_id_or_create(conn, 'sites', in_data.site)
        operator_id: int = await get_id_or_create(conn, 'operators', in_data.operator)
        provider_id: int = await get_id_or_create(conn, 'providers', in_data.provider)
        contractor_id: int = await get_id_or_create(conn, 'contractors', in_data.contractor)
        license_plate_id: int = await get_id_or_create(conn, 'license_plates', in_data.license_plate)
        status_id: int = await get_id_or_create(conn, 'statuses', in_data.status)

        record: asyncpg.Record = await conn.fetchrow(statement, in_data.dt_receiving,
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

    logging.info(f"The guid = {in_data.guid} with ID = {record['id']}  was inserted.")
    return record['id']
