import aiohttp
import asyncio

from http import HTTPStatus
from bs4 import BeautifulSoup

# import re


async def fetch(client: aiohttp.ClientSession, url: str):
    async with client.get(url) as resp:
        assert resp.status == HTTPStatus.OK

        return await resp.text()


# async def login_user(client):
#     url = 'https://pythonscraping.com/pages/files/processing.php'
#     payload = {'firstname': 'Ryan', 'lastname': 'Mitchell'}
#     async with client.post(url, data=payload) as resp:
#         assert resp.status == HTTPStatus.OK
#         return await resp.text()


async def main():
    headers = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'
                   ' Chrome/124.0.0.0 Safari/537.36'}
    async with aiohttp.ClientSession(headers=headers) as session:

        url = 'https://news.mail.ru/politics/60903285/?frommail=1'
        print(await fetch(session, url))







if __name__ == '__main__':
    asyncio.run(main())
