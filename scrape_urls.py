"""
This script only scrapes Yahoo, NYTimes and Guardian. Extend away.
"""

import os
import sys
import csv
import time
import tqdm
import json
import inspect
import asyncio
import aiohttp
import requests
import tldextract
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin


async def async_interface(data, func, max_parallel_connections, colour):
    func_name = func.__name__
    connector = aiohttp.TCPConnector(limit_per_host=max_parallel_connections, limit=max_parallel_connections)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [func(dt, session) for dt in data]
        data = [await f
             for f in tqdm.tqdm(
                 asyncio.as_completed(tasks), 
                 total=len(tasks), 
                 desc=func_name, 
                 colour=colour, 
                 leave=True
                 )]
    return data


# Use this decorator when using async function
# Also takes optional colour argument for progress bar
def to_async(max_parallel_connections=0, colour="#00a86b"):
    def _to_async(func):
        def _setup_func(*args):
            if not inspect.iscoroutinefunction(func):
                return func(*args)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(async_interface(args[0], func, max_parallel_connections, colour))
            return results
        return _setup_func
    if callable(max_parallel_connections):
        return _to_async(max_parallel_connections)
    return _to_async     


def yahoo(data):
    articles = []

    func_name = sys._getframe().f_code.co_name
    pbar = tqdm.tqdm(total=len(data), desc=func_name, colour="#400090", leave=True)

    for dt in data:
        url, sentiment = dt
        time.sleep(0.05)
        text = ""

        try:
            resp = requests.get(url)
        except Exception as e:
            pbar.set_postfix({"Exception": f"{e}"})
            text = "Exception"

        if not text:
            if resp.status_code == 200:
                pbar.set_postfix_str({"Status": f"{resp.status_code}"})
                soup = BeautifulSoup(resp.text, "html.parser")
                text = "".join([i.get_text() for i in soup.find("div", class_="caas-body").find_all("p")])
            else:
                if resp.status_code == 404:
                    pbar.set_postfix_str({"Status": f"{resp.status_code}"})
                else:
                    pbar.set_postfix_str({"Error": f"{resp.status_code}"})
                text = "Exception"
    
        articles.append((url, text, sentiment))
        pbar.update(1)
    return articles


def nytimes(data):
    articles = []

    func_name = sys._getframe().f_code.co_name
    pbar = tqdm.tqdm(total=len(data), desc=func_name, colour="#567b95", leave=True)

    for dt in data:
        url, sentiment = dt
        time.sleep(0.05)
        text = ""

        try:
            resp = requests.get(url)
        except Exception as e:
            pbar.set_postfix({"Exception": f"e"})
            text = "Exception"

        if not text:
            if resp.status_code == 200:
                pbar.set_postfix_str({"Status": f"{resp.status_code}"})
                soup = BeautifulSoup(resp.text, "html.parser")
                for section in soup.find_all("session"):
                    try:
                        if section["name"] == "articleBody":
                            text = section.get_text().strip().replace("\n", "")
                            break
                    except KeyError:
                        pass
            else:
                if resp.status_code == 404:
                    pbar.set_postfix_str({"Status": f"{resp.status_code}"})
                else:
                    pbar.set_postfix_str({"Error": f"{resp.status_code}"})
                text = "Exception"
    
        articles.append((url, text, sentiment))
        pbar.update(1)
    return articles



# NOTE: Only implement scraping logic for one url for async
# NOTE: Session object is injected by the decorator
@to_async(max_parallel_connections=12)
async def theguardian(data, session):
    url, sentiment = data
    NEWS_API_KEY = os.environ["NEWS_API_KEY"]
    BASE_URL = "https://content.guardianapis.com/"
    text = "Exception"
    request_params = {
        "api-key": NEWS_API_KEY, "show-fields": "bodyText"
    }

    path = urlparse(url).path
    news_api = urljoin(BASE_URL, path)

    try:
        r = await session.get(news_api, params=request_params)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        print(f"Got exception for: {url}")
        return url, text, sentiment
    
    if r.status == 200:
        try:
            text = await r.json()
        except json.JSONDecodeError:
            return url, text, sentiment
        
        text = text["response"]["content"]["fields"]["bodyText"]
        soup = BeautifulSoup(text, "html.parser")
        text = soup.get_text()
        return url, text, sentiment



# NOTE: Make sure you add your functions in this dict. If not, your functions will be ignored.
# NOTE: Name of the function MUST be the same as the name of domain you are tryin to scrape
scraper_functions = {
    "yahoo": yahoo,
    "nytimes": nytimes,
    "theguardian": theguardian,
}


# Dispatch each function to its own thread
async def get_articles(data):
    results = await asyncio.gather(*[asyncio.to_thread(scraper_functions[i], data[i]) for i in data.keys()])
    return results


if __name__ == "__main__":
    data = {k:[] for k, v in scraper_functions.items() if v}

    with open(os.getcwd() + '/news_articles.csv', newline="\n") as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            url, sentiment = row
            domain = tldextract.extract(url).domain
            try:
                data[domain].append((url, sentiment))
            except KeyError:
                continue
    
    results = asyncio.run(get_articles(data))
    
    for result in results:
        if len(result):
            with open(os.getcwd() + '/theguardian.csv', "w+", newline="\n") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(result)

