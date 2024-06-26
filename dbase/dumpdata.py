import asyncio
import logging

import asyncpg

import argparse

from datetime import date
from pydantic import ValidationError

from pathlib import Path
import sys

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent

# Importing project packages.
from dbase import data_validators, database
from settings_management import Settings


async def select_from_gsm_table(con: asyncpg.Connection, dt_begin: date, dt_end: date):
    """
    Выводит данные из базы данных("Прием") в стандартный поток вывода.
    """
    stmt = await con.prepare(database.SELECT_GSM_TABLE)
    async with con.transaction():
        print(f"{'Дата приемки'} {'Отчет редакт.':13} {'Дата созд./послед. правки':25} "
              f"{'Участок':32} {'Принято в кг':12} {'Оператор':18} {'Поставщик':42} "
              f"{'Перевозчик':30} {'Гос. номер':20} {'Статус':9}")
        async for record in stmt.cursor(dt_begin, dt_end):
            if record['dt_crch'] == date.min:
                dt_crch = '          '
            else:
                dt_crch = str(record['dt_crch'])
            print(f"{str(record['dt_receiving']):12} {str(record['been_changed']):13} {dt_crch:25} {record['site']:32}"
                  f" {record['income_kg']:<12.2f} {record['operator']:18} {record['provider']:42}"
                  f" {record['contractor']:30} {record['license_plate']:20} {record['status']:9}")


async def select_from_tank_table(con: asyncpg.Connection, dt_begin: date, dt_end: date):
    """
    Выводит данные из базы данных("Выдача в АТЗ") в стандартный поток вывода.
    """
    stmt = await con.prepare(database.SELECT_TANK_TABLE)
    async with con.transaction():
        print(f"{'Дата выдачи'} {'Отчет редакт.':13} {'Дата созд./послед. правки':25} "
              f"{'Участок':32} {'Бортовой номер':50} {'Участок назначения':40} {'Выдано кг':12} {'Статус':9}")
        async for record in stmt.cursor(dt_begin, dt_end):
            if record['dt_crch'] == date.min:
                dt_crch = '          '
            else:
                dt_crch = str(record['dt_crch'])
            print(f"{str(record['dt_giveout']):11} {str(record['been_changed']):13} {dt_crch:25} {record['site']:32} "
                  f"{record['onboard_num']:50} {record['dest_site']:40} {record['given_kg']:<12.2f} "
                  f"{record['status']:9}")


async def select_from_sheet_table(con: asyncpg.Connection, dt_begin: date, dt_end: date):
    """
    Выводит данные из базы данных("Выдача из АТЗ") в стандартный поток вывода.
    """
    stmt = await con.prepare(database.SELECT_SHEET_TABLE)
    async with con.transaction():
        print(f"{'Дата выдачи'} {'Отчет редакт.':13} {'Дата созд./послед. правки':25} "
              f"{'Участок':32} {'АТЗ':30} {'Участок выдачи':40} {'Выдано литров':<13} {'Выдано кг':<12} {'Статус':9}")
        async for record in stmt.cursor(dt_begin, dt_end):
            if record['dt_crch'] == date.min:
                dt_crch = '          '
            else:
                dt_crch = str(record['dt_crch'])
            print(f"{str(record['dt_giveout']):11} {str(record['been_changed']):13} {dt_crch:25} {record['site']:32}"
                  f" {record['atz']:30} {record['give_site']:40} {record['given_litres']:<13.2f}"
                  f" {record['given_kg']:<12.2f} {record['status']:9}")


async def select_from_azs_table(con: asyncpg.Connection, dt_begin: date, dt_end: date):
    """
    Выводит данные из базы данных("Выдача из ТРК") в стандартный поток вывода.
    """
    stmt = await con.prepare(database.SELECT_AZS_TABLE)
    async with con.transaction():
        print(f"{'Дата выдачи'} {'Отчет редакт.':13} {'Дата созд./послед. правки':25} "
              f"{'Участок':32} {'Фио кладовщика':25} {'Счетчик на начало дня':22} {'Счетчик на конец дня':21}"
              f" {'Выдано литров за сутки':23} {'Выдано кг за сутки':19} {'Статус':9}")
        async for record in stmt.cursor(dt_begin, dt_end):
            if record['dt_crch'] == date.min:
                dt_crch = '          '
            else:
                dt_crch = str(record['dt_crch'])
            # print(list(record.keys()))
            print(f"{str(record['dt_giveout']):11} {str(record['been_changed']):13} {dt_crch:25} {record['site']:32}"
                  f" {record['storekeeper']:25} {record['counter_azs_bd']:<22.0f} {record['counter_azs_ed']:<21.0f} "
                  f"{record['given_litres']:<23.2f} {record['given_kg']:<19.2f} {record['status']:9}")


async def select_from_exchange_table(con: asyncpg.Connection, dt_begin: date, dt_end: date):
    """
    Выводит данные из базы данных("Обмен между резервуарами") в стандартный поток вывода.
    """
    stmt = await con.prepare(database.SELECT_EXCHANGE_TABLE)
    async with con.transaction():
        print(f"{'Дата':11} {'Отчет редакт.':13} {'Дата созд./послед. правки':25} "
              f"{'Участок':32} {'ФИО оператора':25} {'Исходный резервуар':28} {'Конечный резервуар':28}"
              f" {'Объем топлива':15} {'Статус':9}")
        async for record in stmt.cursor(dt_begin, dt_end):
            if record['dt_crch'] == date.min:
                dt_crch = '          '
            else:
                dt_crch = str(record['dt_crch'])
            # print(list(record.keys()))
            print(f"{str(record['dt_change']):11} {str(record['been_changed']):13} {dt_crch:25} {record['site']:32}"
                  f" {record['operator']:25} {record['tanker_in']:28} {record['tanker_out']:28} "
                  f"{record['litres']:<15.2f} {record['status']:9}")


async def select_from_remains_table(con: asyncpg.Connection, dt_begin: date, dt_end: date):
    """
    Выводит данные из базы данных("Снятие остатков") в стандартный поток вывода.
    """
    stmt = await con.prepare(database.SELECT_REMAINS_TABLE)
    async with con.transaction():
        print(f"{'Дата':11} {'Участок':32} {'ФИО проверяющего':25} {'Номер емкости':30} {'Остатки (кг)':15} "
              f"{'Марка топлива':15} {'Статус':9}")
        async for record in stmt.cursor(dt_begin, dt_end):

            # print(list(record.keys()))
            print(f"{str(record['dt_inspection']):11} {record['site']:32} {record['inspector']:25} "
                  f"{record['tanker_num']:30} {record['remains_kg']:<15.2f} {record['fuel_mark']:15} "
                  f"{record['status']:9}")


async def main(command: str, date_range: data_validators.DateRangeValid):
    try:
        con_par = Settings()  # Loading environment variables
        connection: asyncpg.Connection = await asyncpg.connect(host=con_par.HOST_DB,
                                                               port=con_par.PORT_DB,
                                                               user=con_par.POSTGRES_USER,
                                                               database=con_par.POSTGRES_DB,
                                                               password=con_par.POSTGRES_PASSWORD.get_secret_value())

        match command:
            case 'gsm':
                await select_from_gsm_table(connection, date_range.dt_begin, date_range.dt_end)
            case 'tank':
                await select_from_tank_table(connection, date_range.dt_begin, date_range.dt_end)
            case 'sheet':
                await select_from_sheet_table(connection, date_range.dt_begin, date_range.dt_end)
            case 'azs':
                await select_from_azs_table(connection, date_range.dt_begin, date_range.dt_end)
            case 'exchange':
                await select_from_exchange_table(connection, date_range.dt_begin, date_range.dt_end)
            case 'remains':
                await select_from_remains_table(connection, date_range.dt_begin, date_range.dt_end)
            case _:
                print('Unknown command!')
    finally:
        await connection.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")

    parser = argparse.ArgumentParser(description='Выводит данные из базы данных в стандартный поток вывода.')
    parser.add_argument('command', choices=('gsm', 'tank', 'sheet', 'azs', 'exchange', 'remains',),
                        help='''1. gsm - Прием. 2. tank - Выдача в АТЗ. 3. sheet - Выдача из АТЗ.
                        4. azs - Выдача из ТРК. 5. exchange - Обмен между резервуарами.
                        6. remains - Снятие остатков. (default=gsm)''')
    parser.add_argument('dt_begin', type=str, default=str(date.today()), nargs='?',
                        help='Начальная дата диапазона в формате YYYY-MM-DD (default=date.today())')
    parser.add_argument('dt_end', type=str, default=str(date.today()), nargs='?',
                        help='Конечная дата диапазона в формате YYYY-MM-DD (default=date.today())')

    args = parser.parse_args()
    try:
        dr = data_validators.DateRangeValid(dt_begin=args.dt_begin, dt_end=args.dt_end)
        asyncio.run(main(command=args.command, date_range=dr))
    except ValidationError as e:
        logging.error(e)
