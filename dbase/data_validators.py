import logging

from datetime import date, datetime
from collections.abc import AsyncIterable
from pydantic import (BaseModel,
                      Field,
                      field_validator,
                      ValidationError)
from typing import Type

import json

import asyncio

# Importing project packages.
from fileapi import fetchdata

# from pathlib import Path
# import sys
# # Defining the paths.
# BASE_DIR = Path(__file__).resolve().parent
# if __name__ == '__main__':
#     sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.parent


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
    def check_valid_income_kg(cls, income_kg: str | float) -> float:
        if income_kg == '':
            return 0.0
        return float(income_kg)

    @field_validator('date_crch', 'dt_receiving')
    @classmethod
    def check_valid_date_color(cls, in_date: str) -> date:
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


async def deserial_valid(raw_data: str, model_val: Type[GsmTable]) -> AsyncIterable:
    """
    Deserialization and validation of data.
    """
    data: list = json.loads(raw_data)['ret']
    for in_data in data:
        try:
            dv = model_val.model_validate(in_data)
        except ValidationError as e:
            logging.error(f'{e}({in_data})')
            continue
        # yield di.model_dump()
        yield dv


async def main():
    logging.info('Start!')
    print(fetchdata.FILE_NAMES[3])
    async for raw_data in fetchdata.fetch_data_file(fetchdata.FILE_NAMES[3]):
        async for data in deserial_valid(raw_data, GsmTable):
            print(data)
    logging.info('Stop!')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
    asyncio.run(main())
