import logging

import aiohttp
import asyncio

# from yarl import URL

import json

from http import HTTPStatus
from bs4 import BeautifulSoup

import sys
from pathlib import Path

# Defining the paths.
BASE_DIR = Path(__file__).resolve().parent
if __name__ == '__main__':
    sys.path.append(str(BASE_DIR.parent))  # Defining PROJECT_DIR = BASE_DIR.paren

# Importing project packages.
from settings_management import Settings

# Defining the main urls.
URL_BASE = 'http://192.168.6.77'
URL_LOG = 'http://192.168.6.77/auth/login/'
URL_GSM = 'http://192.168.6.77/gsm/'

URL_MAIN = {'URL_BASE': 'http://192.168.6.77',
            'URL_LOG': 'http://192.168.6.77/auth/login/',
            'URL_GSM': 'http://192.168.6.77/gsm/', }

# Defining endpoints.
URL_GSM_TABLE = 'http://192.168.6.77/get_gsm_table/'  # Priem
URL_TANK_TABLE = 'http://192.168.6.77/get_tank_table/'  # Vidacha v ATZ
URL_SHEET_TABLE = 'http://192.168.6.77/get_sheet_table/'  # Vidacha iz ATZ
URL_AZS_TABLE = 'http://192.168.6.77/get_azs_table/'  # Vidach iz TRK
URL_REMAINS_TABLE = 'http://192.168.6.77/get_remains_table/'  # Snyatie ostatkov
URL_EXCHANGE_TABLE = 'http://192.168.6.77/get_exchange_table/'  # Obmen megdu rezervuarami
URL_DAILYREP_TABLE = 'http://192.168.6.77/get_dailyrep_table/'  # Sutochmye otchety
URL_OILREP_REP = 'http://192.168.6.77/get_oilrep_table/'  # Vidacha masel
URL_EDITREP_TABLE = 'http://192.168.6.77/get_editrep_table/'
URL_WP_DICT = 'http://192.168.6.77/get_wp_dict/'

URL_ENDPOINT = {'URL_GSM_TABLE': 'http://192.168.6.77/get_gsm_table/',
                'URL_TANK_TABLE': 'http://192.168.6.77/get_tank_table/',
                'URL_SHEET_TABLE': 'http://192.168.6.77/get_sheet_table/',
                'URL_AZS_TABLE': 'http://192.168.6.77/get_azs_table/',
                'URL_REMAINS_TABLE': 'http://192.168.6.77/get_remains_table/',
                'URL_EXCHANGE_TABLE': 'http://192.168.6.77/get_exchange_table/',
                'URL_DAILYREP_TABLE': 'http://192.168.6.77/get_dailyrep_table/',
                'URL_OILREP_REP': 'http://192.168.6.77/get_oilrep_table/',
                'URL_EDITREP_TABLE': 'http://192.168.6.77/get_editrep_table/',
                'URL_WP_DICT': 'http://192.168.6.77/get_wp_dict/', }

# Defining HTTP headers.
HEADERS = {'User-Agent':
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', }


async def login_user(session: aiohttp.ClientSession):
    """
    User registration.
    """
    credent = Settings()
    async with session.get(URL_LOG) as resp:
        if len(resp.history):  # I corrected it here on 06/04/2024
            logging.info(f'It was registered earlier!')
            return None
        logging.info(f'Status code(get): {resp.status}')
        assert resp.status == HTTPStatus.OK
        html_doc = await resp.text()

    soup = BeautifulSoup(html_doc, 'html.parser')
    tag_inp_hidden = soup.find_all('input', attrs={'type': 'hidden'})
    csrm_val_1 = tag_inp_hidden[0].get('value')
    csrm_val_2 = tag_inp_hidden[0].get('value')  # I corrected it here on 06/04/2024

    payload = [('csrfmiddlewaretoken', csrm_val_1),
               ('username',  credent.crm_username), ('password', credent.crm_password.get_secret_value()),
               ('csrfmiddlewaretoken', csrm_val_2)]
    async with session.post(URL_LOG, data=payload) as resp:
        logging.info(f'Status code(post): {resp.status}')
        assert resp.status == HTTPStatus.OK


async def fetch_table(url: str, session: aiohttp.ClientSession, *, dt_beg: str = '', dt_end: str = ''):
    """
    Fetch the data from the endpoint.
    """
    try:
        logging.info(f'Start fetching from the {url}')
        headers = {'Referer': 'http://192.168.6.77/gsm/', }
        ret_prev = []
        page_num = 1
        params = {'filter': json.dumps({"id_wp": "",
                                        "driver_gsm": "",
                                        "dt_beg": dt_beg,
                                        "dt_end": dt_end,
                                        "curent_page": page_num}), }
        while True:
            async with session.get(url, params=params, headers=headers) as resp:
                assert resp.status == HTTPStatus.OK
                json_response = await resp.json()
                text_response = await resp.text()

            if ret_prev == json_response['ret']:
                return
            yield text_response

            ret_prev = json_response['ret']
            page_num += 1
            params = {'filter': json.dumps({"id_wp": "",
                                            "driver_gsm": "",
                                            "dt_beg": dt_beg,
                                            "dt_end": dt_end,
                                            "curent_page": page_num}), }
    finally:
        logging.info(f'Stop  fetching from the {url}')


async def main():
    jar = aiohttp.CookieJar(unsafe=True)
    async with aiohttp.ClientSession(headers=HEADERS, cookie_jar=jar) as session:
        await login_user(session)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")

    logging.info('The program is running!')
    asyncio.run(main())
    logging.info('The program has been stopped!')
