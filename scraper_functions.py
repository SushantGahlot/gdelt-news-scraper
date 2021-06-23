'''
Write scraper functions for individual news websites here
'''

import os
import json
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin


async def yahoo(url, session):
    await asyncio.sleep(0.05)
    text = ""

    try:
        resp = await session.get(url)
    except Exception as e:
        text = "Exception"

    if not text:
        if resp.status == 200:
            text = await resp.text()
            soup = BeautifulSoup(text, "html.parser")
            text = "".join([i.get_text() for i in soup.find(
                "div", class_="caas-body").find_all("p")])
        else:
            if resp.status == 404:
                print("Some shit")  # TODO
            else:
                print("Some other shit")  # TODO
            text = "Exception"
    return text


async def nytimes(url, session):
    await asyncio.sleep(0.05)
    text = ""

    # Do not want urls like -
    # https://www.nytimes.com/live/2021/06/21/business/economy-stock-market-news
    # They contain multiple news articles and sentiment score for such a post 
    # does not reflect the sentiment of each article mentioned in the url
    if "/live/" in url:
        return "Exception"

    try:
        resp = await session.get(url)
    except Exception as e:
        text = "Exception"

    if not text:
        if resp.status == 200:
            text = await resp.text()
            soup = BeautifulSoup(text, "html.parser")
            for section in soup.find_all("section"):
                try:
                    if section["name"] == "articleBody":
                        text = section.get_text().strip().replace("\n", "")
                except KeyError:
                    pass
        else:
            if resp.status == 404:
                pass
            else:
                pass
            text = "Exception"

    return text


async def theguardian(url, session):
    NEWS_API_KEY = os.environ["NEWS_API_KEY"]
    BASE_URL = "https://content.guardianapis.com/"
    request_params = {
        "api-key": NEWS_API_KEY, "show-fields": "bodyText"
    }

    text = ""
    path = urlparse(url).path
    news_api = urljoin(BASE_URL, path)

    try:
        resp = await session.get(news_api, params=request_params)
    except Exception as e:
        text = "Exception"

    if not text:
        if resp.status == 200:
            try:
                text = await resp.json()
            except json.JSONDecodeError:
                return text

            text = text["response"]["content"]["fields"]["bodyText"]
            soup = BeautifulSoup(text, "html.parser")
            text = soup.get_text()
        else:
            if resp.status == 404:
                pass
            else:
                pass
            text = "Exception"
    return text
