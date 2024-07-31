import asyncio
import asyncpg
from asyncpg.connection import Connection

from datetime import date
import pytest
import pytest_asyncio

# Defining the paths.
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent

# Importing project packages.
from dbase import init_db, database, data_validators
from settings_management import Settings

STATEMENT_SELECT = 'SELECT * FROM {0} WHERE id = $1;'
con_par = Settings()  # Loading environment variables

pytestmark = pytest.mark.asyncio(scope="module")
loop: asyncio.AbstractEventLoop


@pytest_asyncio.fixture(scope='module')
async def connection_postgres():
    global loop
    loop = asyncio.get_running_loop()
    # Establish a connection.
    conn: Connection = await asyncpg.connect(host=con_par.HOST_DB,
                                             port=con_par.PORT_DB,
                                             user=con_par.POSTGRES_USER,
                                             database=con_par.POSTGRES_DB,
                                             password=con_par.POSTGRES_PASSWORD.get_secret_value())

    await conn.execute('DROP DATABASE IF EXISTS test_crm;')
    await conn.execute('CREATE DATABASE test_crm;')
    await conn.close()
    conn: Connection = await asyncpg.connect(host=con_par.HOST_DB,
                                             port=con_par.PORT_DB,
                                             user=con_par.POSTGRES_USER,
                                             database='test_crm',
                                             password=con_par.POSTGRES_PASSWORD.get_secret_value())

    await init_db.create_tables(conn)
    yield conn
    # Close the connection.
    await conn.close()
    conn: Connection = await asyncpg.connect(host=con_par.HOST_DB,
                                             port=con_par.PORT_DB,
                                             user=con_par.POSTGRES_USER,
                                             database=con_par.POSTGRES_DB,
                                             password=con_par.POSTGRES_PASSWORD.get_secret_value())
    await conn.execute('DROP DATABASE IF EXISTS test_crm;')
    await conn.close()


async def init_database(connection_postgres: Connection):
    """
    Clearing the test database.
    """
    await init_db.drop_tables(connection_postgres)
    await init_db.create_tables(connection_postgres)


async def test_get_id_or_create(connection_postgres: Connection):
    assert asyncio.get_running_loop() is loop
    rec_id = await database.get_id_or_create(connection_postgres, 'sites', 'SITES_1')
    assert rec_id == 1
    rec_id = await database.get_id_or_create(connection_postgres, 'sites', 'SITES_2')
    assert rec_id == 2
    rec_id = await database.get_id_or_create(connection_postgres, 'sites', 'SITES_1')
    assert rec_id == 1
    await init_database(connection_postgres)  # Clearing the database


test_data_gsm = [({'uch': 'Site_1',
                   'user': 'User_1',
                   'dt_receiving': '12.04.2023',
                   'income_kg': '24976.9',
                   'table_color': 'white',
                   'date_color': '',
                   'provider': 'Provider_1',
                   'contractor': 'Contractor_1',
                   'venchile_gn': 'GN_1',
                   'status': 'Status_2',
                   'status_id': 2,
                   'guid': 'WPGUID29D230412202722S194'},
                  {'gsm_table': {'id': 1,
                                 'dt_receiving': date(2023, 4, 12),
                                 'dt_crch': date(1, 1, 1),
                                 'income_kg': 24976.9,
                                 'been_changed': False,
                                 'guid': 'WPGUID29D230412202722S194'},
                   'sites': {'id': 1, 'name': 'Site_1'},
                   'operators': {'id': 1, 'name': 'User_1'},
                   'providers': {'id': 1, 'name': 'Provider_1'},
                   'contractors': {'id': 1, 'name': 'Contractor_1'},
                   'license_plates': {'id': 1, 'name': 'GN_1'},
                   'statuses': {'id': 1, 'name': 'Status_2'}}),
                 ({'uch': 'Site_2',
                   'user': 'User_2',
                   'dt_receiving': '12.04.2024',
                   'income_kg': '45.91',
                   'table_color': '#f7fcc5',
                   'date_color': '04.07.2024',
                   'provider': 'Provider_2',
                   'contractor': 'Contractor_2',
                   'venchile_gn': 'GN_2',
                   'status': 'Status_2',
                   'status_id': 2,
                   'guid': 'WPGUID29D230412202722S195'},
                  {'gsm_table': {'id': 2,
                                 'dt_receiving': date(2024, 4, 12),
                                 'dt_crch': date(2024, 7, 4),
                                 'income_kg': 45.91,
                                 'been_changed': True,
                                 'guid': 'WPGUID29D230412202722S195'},
                   'sites': {'id': 2, 'name': 'Site_2'},
                   'operators': {'id': 2, 'name': 'User_2'},
                   'providers': {'id': 2, 'name': 'Provider_2'},
                   'contractors': {'id': 2, 'name': 'Contractor_2'},
                   'license_plates': {'id': 2, 'name': 'GN_2'},
                   'statuses': {'id': 1, 'name': 'Status_2'}}),
                 ]


@pytest.mark.parametrize('test_input, expected', test_data_gsm)
async def test_insert_gsm_table(connection_postgres: Connection, test_input, expected):
    assert asyncio.get_running_loop() is loop
    in_data = data_validators.GsmTable(**test_input)
    await database.insert_gsm_table(connection_postgres, in_data)

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('gsm_table'),  expected['gsm_table']['id'])
    assert rec['id'] == expected['gsm_table']['id']
    assert rec['dt_receiving'] == expected['gsm_table']['dt_receiving']
    assert rec['dt_crch'] == expected['gsm_table']['dt_crch']
    assert round(rec['income_kg'], 2) == round(expected['gsm_table']['income_kg'], 2)
    assert rec['been_changed'] == expected['gsm_table']['been_changed']
    assert rec['guid'] == expected['gsm_table']['guid']
    assert rec['site_id'] == expected['sites']['id']
    assert rec['operator_id'] == expected['operators']['id']
    assert rec['provider_id'] == expected['providers']['id']
    assert rec['contractor_id'] == expected['contractors']['id']
    assert rec['license_plate_id'] == expected['license_plates']['id']
    assert rec['status_id'] == expected['statuses']['id']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sites'),
                                             expected['sites']['id'])
    assert rec['id'] == expected['sites']['id']
    assert rec['name'] == expected['sites']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('operators'),
                                             expected['operators']['id'])
    assert rec['id'] == expected['operators']['id']
    assert rec['name'] == expected['operators']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('providers'),
                                             expected['providers']['id'])
    assert rec['id'] == expected['providers']['id']
    assert rec['name'] == expected['providers']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('contractors'),
                                             expected['contractors']['id'])
    assert rec['id'] == expected['contractors']['id']
    assert rec['name'] == expected['contractors']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('license_plates'),
                                             expected['license_plates']['id'])
    assert rec['id'] == expected['license_plates']['id']
    assert rec['name'] == expected['license_plates']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('statuses'),
                                             expected['statuses']['id'])
    assert rec['id'] == expected['statuses']['id']
    assert rec['name'] == expected['statuses']['name']


async def test_clearing_0(connection_postgres: Connection):
    assert asyncio.get_running_loop() is loop
    await init_database(connection_postgres)


test_data_tank = [({'dt_giveout_t': '12.07.2024',
                    'uch': 'SITE_1',
                    'venchile_gn_t': 'ONBOARD_NUM_1',
                    'wp_dest_t': 'SITE_2',
                    'given_mass_t': '3880.8',
                    'table_color': 'white',
                    'date_color': '',
                    'status': 'STATUS_1',
                    'status_id': 2,
                    'guid': 'WPGUID15D240712184728S706'
                    },
                   {'tank_table': {'id': 1,
                                   'dt_giveout': date(2024, 7, 12),
                                   'dt_crch': date(1, 1, 1),
                                   'given_kg': 3880.8,
                                   'been_changed': False,
                                   'guid': 'WPGUID15D240712184728S706'},
                    'sites': {'id': 1, 'name': 'SITE_1'},
                    'onboard_nums': {'id': 1, 'name': 'ONBOARD_NUM_1'},
                    'dest_sites': {'id': 2, 'name': 'SITE_2'},
                    'statuses': {'id': 1, 'name': 'STATUS_1'}
                    }
                   ),
                  ({'dt_giveout_t': '12.08.2024',
                    'uch': 'SITE_3',
                    'venchile_gn_t': 'ONBOARD_NUM_2',
                    'wp_dest_t': 'SITE_4',
                    'given_mass_t': '555.78',
                    'table_color': '#f7fcc5',
                    'date_color': '05.07.2024',
                    'status': 'STATUS_1',
                    'status_id': 2,
                    'guid': 'WPGUID15D240712184728S733'
                    },
                   {'tank_table': {'id': 2,
                                   'dt_giveout': date(2024, 8, 12),
                                   'dt_crch': date(2024, 7, 5),
                                   'given_kg': 555.78,
                                   'been_changed': True,
                                   'guid': 'WPGUID15D240712184728S733'},
                    'sites': {'id': 3, 'name': 'SITE_3'},
                    'onboard_nums': {'id': 2, 'name': 'ONBOARD_NUM_2'},
                    'dest_sites': {'id': 4, 'name': 'SITE_4'},
                    'statuses': {'id': 1, 'name': 'STATUS_1'}
                    }
                   ),
                  ]


@pytest.mark.parametrize('test_input, expected', test_data_tank)
async def test_insert_tank_table(connection_postgres: Connection, test_input, expected):
    assert asyncio.get_running_loop() is loop
    in_data = data_validators.TankTable(**test_input)
    await database.insert_tank_table(connection_postgres, in_data)

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('tank_table'),
                                             expected['tank_table']['id'])
    assert rec['id'] == expected['tank_table']['id']
    assert rec['dt_giveout'] == expected['tank_table']['dt_giveout']
    assert rec['dt_crch'] == expected['tank_table']['dt_crch']
    assert round(rec['given_kg'], 2) == round(expected['tank_table']['given_kg'], 2)
    assert rec['been_changed'] == expected['tank_table']['been_changed']
    assert rec['guid'] == expected['tank_table']['guid']
    assert rec['site_id'] == expected['sites']['id']
    assert rec['onboard_num_id'] == expected['onboard_nums']['id']
    assert rec['dest_site_id'] == expected['dest_sites']['id']
    assert rec['status_id'] == expected['statuses']['id']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sites'),
                                             expected['sites']['id'])
    assert rec['id'] == expected['sites']['id']
    assert rec['name'] == expected['sites']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('onboard_nums'),
                                             expected['onboard_nums']['id'])
    assert rec['id'] == expected['onboard_nums']['id']
    assert rec['name'] == expected['onboard_nums']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sites'),
                                             expected['dest_sites']['id'])
    assert rec['id'] == expected['dest_sites']['id']
    assert rec['name'] == expected['dest_sites']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('statuses'),
                                             expected['statuses']['id'])
    assert rec['id'] == expected['statuses']['id']
    assert rec['name'] == expected['statuses']['name']


async def test_clearing_1(connection_postgres: Connection):
    assert asyncio.get_running_loop() is loop
    await init_database(connection_postgres)


test_data_sheet = [({'date_s': '02.06.2024',
                     'storekeeper_name_s': 'SITE_1',
                     'shift_s': 'MORNING',
                     'atz_tanker_sheet': 'ATZ_1',
                     'uch_s': 'SITE_2',
                     'given_litres_s': 885.0,
                     'given_kg_s': 744.3,
                     'status': 'STATUS_1',
                     'status_id': 2,
                     'guid': 'WPGUID7D240603113821SH556',
                     'date_color': '',
                     'table_color': 'white'},
                    {'sheet_table': {'id': 1,
                                     'dt_giveout': date(2024, 6, 2),
                                     'dt_crch': date(1, 1, 1),
                                     'given_litres': 885.0,
                                     'given_kg': 744.3,
                                     'been_changed': False,
                                     'guid': 'WPGUID7D240603113821SH556'},
                     'sites': {'id': 1, 'name': 'SITE_1'},
                     'atzs': {'id': 1, 'name': 'ATZ_1'},
                     'sites_give': {'id': 2, 'name': 'SITE_2'},
                     'statuses': {'id': 1, 'name': 'STATUS_1'}}),
                   ({'date_s': '02.06.2023',
                     'storekeeper_name_s': 'SITE_3',
                     'shift_s': 'MORNING',
                     'atz_tanker_sheet': 'ATZ_2',
                     'uch_s': 'SITE_2',
                     'given_litres_s': 885.40,
                     'given_kg_s': 744.35,
                     'status': 'STATUS_1',
                     'status_id': 2,
                     'guid': 'WPGUID7D240603113821SH552',
                     'date_color': '04.06.2024',
                     'table_color': '#f7fcc5'},
                    {'sheet_table': {'id': 2,
                                     'dt_giveout': date(2023, 6, 2),
                                     'dt_crch': date(2024, 6, 4),
                                     'given_litres': 885.40,
                                     'given_kg': 744.35,
                                     'been_changed': True,
                                     'guid': 'WPGUID7D240603113821SH552'},
                     'sites': {'id': 3, 'name': 'SITE_3'},
                     'atzs': {'id': 2, 'name': 'ATZ_2'},
                     'sites_give': {'id': 2, 'name': 'SITE_2'},
                     'statuses': {'id': 1, 'name': 'STATUS_1'}
                     }
                    ),
                   ]


@pytest.mark.parametrize('test_input, expected', test_data_sheet)
async def test_insert_sheet_table(connection_postgres: Connection, test_input, expected):
    assert asyncio.get_running_loop() is loop
    in_data = data_validators.SheetTable(**test_input)
    await database.insert_sheet_table(connection_postgres, in_data)

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sheet_table'),
                                             expected['sheet_table']['id'])
    assert rec['id'] == expected['sheet_table']['id']
    assert rec['dt_giveout'] == expected['sheet_table']['dt_giveout']
    assert rec['dt_crch'] == expected['sheet_table']['dt_crch']
    assert round(rec['given_litres'], 2) == round(expected['sheet_table']['given_litres'], 2)
    assert round(rec['given_kg'], 2) == round(expected['sheet_table']['given_kg'], 2)
    assert rec['been_changed'] == expected['sheet_table']['been_changed']
    assert rec['guid'] == expected['sheet_table']['guid']
    assert rec['site_id'] == expected['sites']['id']
    assert rec['atz_id'] == expected['atzs']['id']
    assert rec['give_site_id'] == expected['sites_give']['id']
    assert rec['status_id'] == expected['statuses']['id']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sites'),
                                             expected['sites']['id'])
    assert rec['id'] == expected['sites']['id']
    assert rec['name'] == expected['sites']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('atzs'),
                                             expected['atzs']['id'])
    assert rec['id'] == expected['atzs']['id']
    assert rec['name'] == expected['atzs']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sites'),
                                             expected['sites_give']['id'])
    assert rec['id'] == expected['sites_give']['id']
    assert rec['name'] == expected['sites_give']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('statuses'),
                                             expected['statuses']['id'])
    assert rec['id'] == expected['statuses']['id']
    assert rec['name'] == expected['statuses']['name']


async def test_clearing_2(connection_postgres: Connection):
    assert asyncio.get_running_loop() is loop
    await init_database(connection_postgres)

test_data_azs = [({"uch": 'SITE_1',
                   'dt_giveout_a': '12.07.2024',
                   'storekeeper_name_a': 'STOREKEEPER_1',
                   'given_litres_a': 606,
                   'given_kg_a': '504.2',
                   'table_color': 'white',
                   'date_color': '',
                   'counter_azs_begin_day_a': 171270,
                   'counter_azs_end_day': 171876,
                   'status': 'STATUS_1',
                   'status_id': 2,
                   'guid': 'WPGUID67D240712182756S120'},
                  {'azs_table': {'id': 1,
                                 'dt_giveout': date(2024, 7,  12),
                                 'dt_crch': date(1, 1, 1),
                                 'counter_azs_bd': 171270,
                                 'counter_azs_ed': 171876,
                                 'given_litres': 606,
                                 'given_kg': 504.2,
                                 'been_changed': False,
                                 'guid': 'WPGUID67D240712182756S120'},
                   'sites': {'id': 1, 'name': 'SITE_1'},
                   'storekeepers': {'id': 1, 'name': 'STOREKEEPER_1'},
                   'statuses': {'id': 1, 'name': 'STATUS_1'}
                   }
                  ),
                 ({"uch": 'SITE_2',
                   'dt_giveout_a': '12.04.2024',
                   'storekeeper_name_a': 'STOREKEEPER_2',
                   'given_litres_a': 233,
                   'given_kg_a': '504.22',
                   'table_color': '#f7fcc5',
                   'date_color': '01.10.2024',
                   'counter_azs_begin_day_a': 71270,
                   'counter_azs_end_day': 71876,
                   'status': 'STATUS_2',
                   'status_id': 2,
                   'guid': 'WPGUID67D240712182756S12022'},
                  {'azs_table': {'id': 2,
                                 'dt_giveout': date(2024, 4,  12),
                                 'dt_crch': date(2024, 10, 1),
                                 'counter_azs_bd': 71270,
                                 'counter_azs_ed': 71876,
                                 'given_litres': 233,
                                 'given_kg': 504.22,
                                 'been_changed': True,
                                 'guid': 'WPGUID67D240712182756S12022'},
                   'sites': {'id': 2, 'name': 'SITE_2'},
                   'storekeepers': {'id': 2, 'name': 'STOREKEEPER_2'},
                   'statuses': {'id': 2, 'name': 'STATUS_2'}
                   }
                  ),
                 ]


@pytest.mark.parametrize('test_input, expected', test_data_azs)
async def test_insert_azs_table(connection_postgres: Connection, test_input, expected):
    assert asyncio.get_running_loop() is loop
    in_data = data_validators.AZSTable(**test_input)
    await database.insert_azs_table(connection_postgres, in_data)

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('azs_table'),
                                             expected['azs_table']['id'])
    assert rec['id'] == expected['azs_table']['id']
    assert rec['dt_giveout'] == expected['azs_table']['dt_giveout']
    assert rec['dt_crch'] == expected['azs_table']['dt_crch']
    assert round(rec['given_litres'], 2) == round(expected['azs_table']['given_litres'], 2)
    assert round(rec['given_kg'], 2) == round(expected['azs_table']['given_kg'], 2)
    assert round(rec['counter_azs_bd'], 0) == round(expected['azs_table']['counter_azs_bd'], 0)
    assert round(rec['counter_azs_ed'], 0) == round(expected['azs_table']['counter_azs_ed'], 0)
    assert rec['been_changed'] == expected['azs_table']['been_changed']
    assert rec['guid'] == expected['azs_table']['guid']
    assert rec['site_id'] == expected['sites']['id']
    assert rec['storekeeper_id'] == expected['storekeepers']['id']
    assert rec['status_id'] == expected['statuses']['id']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sites'),
                                             expected['sites']['id'])
    assert rec['id'] == expected['sites']['id']
    assert rec['name'] == expected['sites']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('storekeepers'),
                                             expected['sites']['id'])
    assert rec['id'] == expected['storekeepers']['id']
    assert rec['name'] == expected['storekeepers']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('statuses'),
                                             expected['statuses']['id'])
    assert rec['id'] == expected['statuses']['id']
    assert rec['name'] == expected['statuses']['name']


async def test_clearing_3(connection_postgres: Connection):
    assert asyncio.get_running_loop() is loop
    await init_database(connection_postgres)


test_data_exchange = [({'uch': 'SITE_1',
                        'dt_change_tc': '12.07.2024',
                        "storekeeper_name_tc": 'OPERATOR_1',
                        'tanker_in_tc': 'TANKER_1',
                        'table_color': 'white',
                        'date_color': '',
                        'tanker_out_tc': 'TANKER_2',
                        'litres_tc': 57928,
                        'density_tc': 0.83,
                        'status': 'STATUS_1',
                        'status_id': 2,
                        'guid': 'WPGUID29D240712161326TCH193'},
                       {'exchange_table': {'id': 1,
                                           'dt_change': date(2024, 7, 12),
                                           'dt_crch': date(1, 1, 1),
                                           'litres': 57928,
                                           'density': 0.83,
                                           'been_changed': False,
                                           'guid': 'WPGUID29D240712161326TCH193'},
                        'sites': {'id': 1, 'name': 'SITE_1'},
                        'operators': {'id': 1, 'name': 'OPERATOR_1'},
                        'tankers_in': {'id': 1, 'name': 'TANKER_1'},
                        'tankers_out': {'id': 2, 'name': 'TANKER_2'},
                        'statuses': {'id': 1, 'name': 'STATUS_1'}
                        }
                       ),
                      ({'uch': 'SITE_2',
                        'dt_change_tc': '10.03.2024',
                        "storekeeper_name_tc": 'OPERATOR_2',
                        'tanker_in_tc': 'TANKER_3',
                        'table_color': '#f7fcc5',
                        'date_color': '09.07.2024',
                        'tanker_out_tc': 'TANKER_4',
                        'litres_tc': 57,
                        'density_tc': 0.72,
                        'status': 'STATUS_2',
                        'status_id': 2,
                        'guid': 'WPGUID29D240712161326TCH'},
                       {'exchange_table': {'id': 2,
                                           'dt_change': date(2024, 3, 10),
                                           'dt_crch': date(2024, 7, 9),
                                           'litres': 57,
                                           'density': 0.72,
                                           'been_changed': True,
                                           'guid': 'WPGUID29D240712161326TCH'},
                        'sites': {'id': 2, 'name': 'SITE_2'},
                        'operators': {'id': 2, 'name': 'OPERATOR_2'},
                        'tankers_in': {'id': 3, 'name': 'TANKER_3'},
                        'tankers_out': {'id': 4, 'name': 'TANKER_4'},
                        'statuses': {'id': 2, 'name': 'STATUS_2'}
                        }
                       ),
                      ]


@pytest.mark.parametrize('test_input, expected', test_data_exchange)
async def test_insert_exchange_table(connection_postgres: Connection, test_input, expected):
    assert asyncio.get_running_loop() is loop
    in_data = data_validators.ExchangeTable(**test_input)
    await database.insert_exchange_table(connection_postgres, in_data)

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('exchange_table'),
                                             expected['exchange_table']['id'])
    assert rec['id'] == expected['exchange_table']['id']
    assert rec['dt_change'] == expected['exchange_table']['dt_change']
    assert rec['dt_crch'] == expected['exchange_table']['dt_crch']
    assert round(rec['litres'], 2) == round(expected['exchange_table']['litres'], 2)
    assert round(rec['density'], 2) == round(expected['exchange_table']['density'], 2)
    assert rec['been_changed'] == expected['exchange_table']['been_changed']
    assert rec['guid'] == expected['exchange_table']['guid']
    assert rec['site_id'] == expected['sites']['id']
    assert rec['tanker_in_id'] == expected['tankers_in']['id']
    assert rec['tanker_out_id'] == expected['tankers_out']['id']
    assert rec['status_id'] == expected['statuses']['id']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sites'),
                                             expected['sites']['id'])
    assert rec['id'] == expected['sites']['id']
    assert rec['name'] == expected['sites']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('operators'),
                                             expected['operators']['id'])
    assert rec['id'] == expected['operators']['id']
    assert rec['name'] == expected['operators']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('tankers'),
                                             expected['tankers_in']['id'])
    assert rec['id'] == expected['tankers_in']['id']
    assert rec['name'] == expected['tankers_in']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('tankers'),
                                             expected['tankers_out']['id'])
    assert rec['id'] == expected['tankers_out']['id']
    assert rec['name'] == expected['tankers_out']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('statuses'),
                                             expected['statuses']['id'])
    assert rec['id'] == expected['statuses']['id']
    assert rec['name'] == expected['statuses']['name']


async def test_clearing_4(connection_postgres: Connection):
    assert asyncio.get_running_loop() is loop
    await init_database(connection_postgres)

test_data_remains = [({'uch': 'SITE_1',
                       'dt_inspection_r': '09.07.2024',
                       'inspector_fio_r': 'INSPECTOR_1',
                       'section_number_r': 'TANKER_1',
                       'correction': 0,
                       'remains_kg_r': 2700,
                       'fuel_mark_r': 'FUEL_1',
                       'status': 'STATUS_1',
                       'status_id': 2,
                       'guid': 'WPGUID24D240709143653S56'
                       },
                      {'remains_table': {'id': 1,
                                         'dt_inspection': date(2024, 7, 9),
                                         'remains_kg': 2700,
                                         'guid': 'WPGUID24D240709143653S56'},
                       'sites': {'id': 1, 'name': 'SITE_1'},
                       'inspectors': {'id': 1, 'name': 'INSPECTOR_1'},
                       'tankers': {'id': 1, 'name': 'TANKER_1'},
                       'fuel_marks': {'id': 1, 'name': 'FUEL_1'},
                       'statuses': {'id': 1, 'name': 'STATUS_1'}
                       }
                      ),
                     ]


@pytest.mark.parametrize('test_input, expected', test_data_remains)
async def test_insert_remains_table(connection_postgres: Connection, test_input, expected):
    assert asyncio.get_running_loop() is loop
    in_data = data_validators.RemainsTable(**test_input)
    await database.insert_remains_table(connection_postgres, in_data)

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('remains_table'),
                                             expected['remains_table']['id'])
    assert rec['id'] == expected['remains_table']['id']
    assert rec['dt_inspection'] == expected['remains_table']['dt_inspection']
    assert round(rec['remains_kg'], 2) == round(expected['remains_table']['remains_kg'], 2)
    assert rec['guid'] == expected['remains_table']['guid']
    assert rec['site_id'] == expected['sites']['id']
    assert rec['inspector_id'] == expected['inspectors']['id']
    assert rec['tanker_num_id'] == expected['tankers']['id']
    assert rec['fuel_mark_id'] == expected['fuel_marks']['id']
    assert rec['status_id'] == expected['statuses']['id']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('sites'),
                                             expected['sites']['id'])
    assert rec['id'] == expected['sites']['id']
    assert rec['name'] == expected['sites']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('inspectors'),
                                             expected['inspectors']['id'])
    assert rec['id'] == expected['inspectors']['id']
    assert rec['name'] == expected['inspectors']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('tankers'),
                                             expected['tankers']['id'])
    assert rec['id'] == expected['tankers']['id']
    assert rec['name'] == expected['tankers']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('fuel_marks'),
                                             expected['fuel_marks']['id'])
    assert rec['id'] == expected['fuel_marks']['id']
    assert rec['name'] == expected['fuel_marks']['name']

    rec = await connection_postgres.fetchrow(STATEMENT_SELECT.format('statuses'),
                                             expected['statuses']['id'])
    assert rec['id'] == expected['statuses']['id']
    assert rec['name'] == expected['statuses']['name']
