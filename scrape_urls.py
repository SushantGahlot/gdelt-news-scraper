"""
This script is case specific for Yahoo, NYTimes and Guardian
"""

import asyncio
from os import utime
from bs4 import BeautifulSoup
import aiohttp
import csv
import time
import tldextract
import requests
import concurrent.futures


def yahoo(data):
    articles = []

    for url, sentiment in data:
        time.sleep(0.1)
        text = ""

        try:
            resp = requests.get(url)
        except Exception as e:
            print(f"Got exception {e} for {url}")

        if not text:
            if resp.status_code == 200:
                print(f"Got result for url: {url}")
                soup = BeautifulSoup(resp.text, "html.parser")
                text = "".join([i.get_text() for i in soup.find("div", class_="caas-body").find_all("p")])
            elif resp.status_code == 404:
                print(f"Got 404 result for url: {url}")
            else:
                print(f"Got no result for url: {url}")
    
        articles.append(url, text, sentiment)
    return articles


def nytimes(data):
    # TODO: Implement this
    return []

def theguardian(data):
    # TODO: Implement this
    return []


# NOTE: Make sure you add your function in this dict. If not, your function will be ignored.
# NOTE: Keep the name of the function as the name of domain you are tryin to scrape
# This is as loosely coupled as it gets without overengineering
scraper_functions = {
    "yahoo": yahoo,
    "nytimes": nytimes,
    "theguardian": theguardian,
}


async def get_articles(data):
    results = await asyncio.gather(*[asyncio.to_thread(scraper_functions[i], data[i]) for i in data.keys()])
    return results


if __name__ == "__main__":

    data = {k:[] for k, v in scraper_functions.items() if v}

    with open('./news_articles.csv', newline="\n") as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            url, sentiment = row
            domain = tldextract.extract(url).domain
            try:
                data[domain].append((url, sentiment))
            except KeyError:
                print(f"Domain {domain} not found in base dict.")
    
    
    results = asyncio.run(get_articles(data))
