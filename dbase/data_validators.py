import logging

from datetime import date, datetime

from collections.abc import AsyncIterable
from typing import Union

from pydantic import (BaseModel,
                      Field,
                      field_validator,
                      model_validator,
                      ValidationError)
from typing import Type
from typing_extensions import Self

import json

import asyncio

from pathlib import Path
import sys
# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent

# Importing project packages.
from filestools import fileapi


# Defining classes to validate input data.
class GsmTable(BaseModel):
    """Priem"""
    dt_receiving: date | str  # Data priemki
    date_crch: str | date = Field(validation_alias='date_color')  # Data sozdaniya ili posledney pravki
    site: str = Field(validation_alias='uch')  # Uchastok
    income_kg: float | str  # Prinyato v kg
    operator: str = Field(validation_alias='user')  # Operator
    provider: str  # Postavshik
    contractor: str  # Perevozshik
    license_plate: str = Field(validation_alias='venchile_gn')   # Gos nomer
    status: str  # Zagruzgen
    been_changed: bool | str = Field(validation_alias='table_color')  # table_color = '#f7fcc5' = True
    status_id: int = Field(exclude=True)
    guid: str

    @field_validator('income_kg')
    @classmethod
    def check_valid_data(cls, kg: str | float) -> float:
        if kg == '':
            return 0.0
        return float(kg)

    @field_validator('date_crch', 'dt_receiving')
    @classmethod
    def check_valid_date(cls, in_date: str) -> date:
        if in_date == '':
            return date.min
        out_date = datetime.strptime(in_date, '%d.%m.%Y').date()
        return out_date

    @field_validator('been_changed')
    @classmethod
    def check_valid_table_color(cls, in_data: str) -> bool:
        if in_data == '#f7fcc5':
            return True
        return False


class TankTable(BaseModel):
    """Vidacha v ATZ"""
    dt_giveout: date | str = Field(validation_alias='dt_giveout_t')  # Data vidachi
    date_crch: date | str = Field(validation_alias='date_color')  # Data sozdaniya ili posledney pravki
    site: str = Field(validation_alias='uch')  # Uchastok
    onboard_num:  str = Field(validation_alias='venchile_gn_t')  # Bortovoi nomer
    dest_site: str = Field(validation_alias='wp_dest_t')  # Uchastok naznacheniya
    given_kg: float | str = Field(validation_alias='given_mass_t')  # Vidano v kg
    status: str  # Zagruzgen
    been_changed: bool | str = Field(validation_alias='table_color')  # table_color = '#f7fcc5' = True
    status_id: int = Field(exclude=True)
    guid: str

    @field_validator('given_kg')
    @classmethod
    def check_valid_data(cls, kg: str | float) -> float:
        if kg == '':
            return 0.0
        return float(kg)

    @field_validator('date_crch', 'dt_giveout')
    @classmethod
    def check_valid_date(cls, in_date: str) -> date:
        if in_date == '':
            return date.min
        out_date = datetime.strptime(in_date, '%d.%m.%Y').date()
        return out_date

    @field_validator('been_changed')
    @classmethod
    def check_valid_table_color(cls, in_data: str) -> bool:
        if in_data == '#f7fcc5':
            return True
        return False


class SheetTable(BaseModel):
    """Vidacha  iz ATZ"""
    dt_giveout: date | str = Field(validation_alias='date_s')  # Data vidachi
    date_crch: date | str = Field(validation_alias='date_color')  # Data sozdaniya ili posledney pravki
    site: str = Field(validation_alias='storekeeper_name_s')  # Uchastok
    shift_s: str = Field(exclude=True)  # Utro or Vecher
    atz: str = Field(validation_alias='atz_tanker_sheet')  # ATZ
    give_site: str = Field(validation_alias='uch_s')  # Uchastok vidachi
    given_litres: float | str = Field(validation_alias='given_litres_s')  # Vidano v litres
    given_kg: float | str = Field(validation_alias='given_kg_s')  # Vidano v kg
    status: str  # Zagruzgen
    been_changed: bool | str = Field(validation_alias='table_color')  # table_color = '#f7fcc5' = True
    status_id: int = Field(exclude=True)
    guid: str

    @field_validator('date_crch', 'dt_giveout')
    @classmethod
    def check_valid_date(cls, in_date: str) -> date:
        if in_date == '':
            return date.min
        out_date = datetime.strptime(in_date, '%d.%m.%Y').date()
        return out_date

    @field_validator('given_kg', 'given_litres')
    @classmethod
    def check_valid_data(cls, kg: str | float) -> float:
        if kg == '':
            return 0.0
        return float(kg)

    @field_validator('been_changed')
    @classmethod
    def check_valid_table_color(cls, in_data: str) -> bool:
        if in_data == '#f7fcc5':
            return True
        return False


class AZSTable(BaseModel):
    """Vidacha  iz TRK"""
    dt_giveout: date | str = Field(validation_alias='dt_giveout_a')  # Data vidachi
    date_crch: date | str = Field(validation_alias='date_color')  # Data sozdaniya ili posledney pravki
    site: str = Field(validation_alias='uch')  # Uchastok
    storekeeper_name: str = Field(validation_alias='storekeeper_name_a')  # FIO kladovchika
    counter_azs_bd: float | str = Field(validation_alias='counter_azs_begin_day_a')  # Shetchik nachalo dnya
    counter_azs_ed: float | str = Field(validation_alias='counter_azs_end_day')  # Shetchik konec dnya
    given_litres: float | str = Field(validation_alias='given_litres_a')  # Vidano v litres za sutki
    given_kg: float | str = Field(validation_alias='given_kg_a')  # Vidano v kg za sutki
    status: str  # Zagruzgen
    been_changed: bool | str = Field(validation_alias='table_color')  # table_color = '#f7fcc5' = True
    status_id: int = Field(exclude=True)
    guid: str

    @field_validator('date_crch', 'dt_giveout')
    @classmethod
    def check_valid_date(cls, in_date: str) -> date:
        if in_date == '':
            return date.min
        out_date = datetime.strptime(in_date, '%d.%m.%Y').date()
        return out_date

    @field_validator('counter_azs_bd', 'counter_azs_ed', 'given_kg', 'given_litres')
    @classmethod
    def check_valid_data(cls, kg: str | float) -> float:
        if kg == '':
            return 0.0
        return float(kg)

    @field_validator('been_changed')
    @classmethod
    def check_valid_table_color(cls, in_data: str) -> bool:
        if in_data == '#f7fcc5':
            return True
        return False


class ExchangeTable(BaseModel):
    """Obmen megdu rezervuarami"""
    dt_change: date | str = Field(validation_alias='dt_change_tc')  # Data obmena
    date_crch: date | str = Field(validation_alias='date_color')  # Data sozdaniya ili posledney pravki
    site: str = Field(validation_alias='uch')  # Uchastok
    operator: str = Field(validation_alias='storekeeper_name_tc')  # Operator
    tanker_in: str | int = Field(validation_alias='tanker_in_tc')  # Ishodniy rezervuar
    tanker_out: str | int = Field(validation_alias='tanker_out_tc')  # Konechniy rezervuar
    litres: float | str = Field(validation_alias='litres_tc')  # Obyom topliva
    density: float | str = Field(validation_alias='density_tc')  # Plotnost (v tablice ne pokazivaet)
    been_changed: bool | str = Field(validation_alias='table_color')  # table_color = '#f7fcc5' = True
    status: str  # Zagruzgen
    status_id: int = Field(exclude=True)
    guid: str

    @field_validator('date_crch', 'dt_change')
    @classmethod
    def check_valid_date(cls, in_date: str) -> date:
        if in_date == '':
            return date.min
        out_date = datetime.strptime(in_date, '%d.%m.%Y').date()
        return out_date


    @field_validator('litres', 'density')
    @classmethod
    def check_valid_data(cls, in_d: str | float) -> float:
        if in_d == '':
            return 0.0
        return float(in_d)

    @field_validator('been_changed')
    @classmethod
    def check_valid_table_color(cls, in_data: str) -> bool:
        if in_data == '#f7fcc5':
            return True
        return False

    @field_validator('tanker_in', 'tanker_out')
    @classmethod
    def check_valid_tanker(cls, tanker: str | int) -> str:
        return str(tanker)


class RemainsTable(BaseModel):
    """Snyatie ostatkov"""
    dt_inspection: date | str = Field(validation_alias='dt_inspection_r')  # Data obmena
    site: str = Field(validation_alias='uch')  # Uchastok
    inspector: str = Field(validation_alias='inspector_fio_r')  # FIO proveryayshego
    tanker_num: str | int = Field(validation_alias='section_number_r')  # Nomer emkosti
    remains_kg: float | str = Field(validation_alias='remains_kg_r')  # Ostatki (kg)
    fuel_mark: str = Field(validation_alias='fuel_mark_r')  # Marka topliva
    status: str  # Zagruzgen
    status_id: int = Field(exclude=True)
    guid: str
    correction: int = Field(exclude=True)

    @field_validator('dt_inspection')
    @classmethod
    def check_valid_date(cls, in_date: str) -> date:
        if in_date == '':
            return date.min
        out_date = datetime.strptime(in_date, '%d.%m.%Y').date()
        return out_date

    @field_validator('remains_kg')
    @classmethod
    def check_valid_data(cls, in_d: str | float) -> float:
        if in_d == '':
            return 0.0
        return float(in_d)

    @field_validator('tanker_num')
    @classmethod
    def check_valid_tanker(cls, tanker: str | int) -> str:
        return str(tanker)


class DateRangeValid(BaseModel):
    dt_begin: date
    dt_end: date

    @model_validator(mode='after')
    def check_date(self) -> Self:
        if self.dt_begin > self.dt_end:
            raise ValueError('dt_end должно быть больше или равно dt_begin!')
        return self






async def deserial_valid(raw_data: str, model_val: Union[Type[GsmTable], Type[TankTable],
                                                         Type[SheetTable], Type[AZSTable],
                                                         Type[ExchangeTable], Type[RemainsTable]]
                         ) -> AsyncIterable[Union[GsmTable, TankTable, SheetTable,
                                                  AZSTable, ExchangeTable, RemainsTable]]:
    """
    Deserialization and validation of data.
    """
    data: list = json.loads(raw_data)['ret']  # ERROR !!????????
    for in_data in data:
        try:
            dv = model_val.model_validate(in_data)
            yield dv
        except ValidationError as e:
            logging.error(f'{e}({in_data})')
            continue
        # yield dv.model_dump()





async def main():
    logging.info('Start!')
    # print(fileapi.FILE_NAMES[3])
    # async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[3]):
    #     async for data in deserial_valid(raw_data, GsmTable):
    #         print(data)

    print(fileapi.FILE_NAMES[7])
    async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[7]):
        async for data in deserial_valid(raw_data, TankTable):
            print(data)

    # print(fileapi.FILE_NAMES[6])
    # async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[6]):
    #     async for data in deserial_valid(raw_data, SheetTable):
    #         print(data)

    # print(fileapi.FILE_NAMES[0])
    # async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[0]):
    #     async for data in deserial_valid(raw_data, AZSTable):
    #         print(data)

    # print(fileapi.FILE_NAMES[2])
    # async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[2]):
    #     async for data in deserial_valid(raw_data, ExchangeTable):
    #         print(data)

    # print(fileapi.FILE_NAMES[5])
    # async for raw_data in fileapi.fetch_data_file(fileapi.FILE_NAMES[5]):
    #     async for data in deserial_valid(raw_data, RemainsTable):
    #         print(data)
    logging.info('Stop!')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    asyncio.run(main())
