from multiprocessing.context import Process
from multiprocessing import Queue
import queue
import urllib
import asyncio
import multiprocessing
from gzip import decompress
import os
import yaml
import csv
import time
import aiohttp

def producer(queue, shared_list):
    gdelt_url = "http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/MASTERFILELIST.TXT"
    resp = urllib.request.urlopen(gdelt_url).read().decode("utf-8")
    
    urls = reversed([i for i in resp.split("\n") if len(i)])
    urls = list(urls)
    urls = urls[int(len(urls)/2):]

    now = time.time()
    for url in urls:
        if len(shared_list) >= 10000:
            for i in range(multiprocessing.cpu_count()):
                print("Putting the killing pill in the Queue.")
                queue.put(None)
            break
        queue.put(url)


async def get_data(url, session):
    print("Fetching data from %s in process - %s" %(url, os.getpid()))
    try:
        r = await session.request("GET", url=url)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return
    return await r.read()
        


async def create_tasks(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [get_data(dt, session) for dt in urls]
        data = await asyncio.gather(*tasks)
    return data


def consumer(queue, shared_list):
    news_sources = ["www.theguardian.com", "://news.yahoo.com", "://www.nytimes.com", "://thehindu.com"]
    # news_source = "www.theguardian.com"
    urls = []
    
    while True:
        url = queue.get()
        
        if url is None:
            print("Goodbye from process %s" % os.getpid())
            break 
        
        urls.append(url)
        print(url)
        
        if len(urls) >= 10:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(create_tasks(urls))

            print("Fetched all data. Got %s results. In process %s" %(len(results), os.getpid()))

            for response in results:
                if not len(response):
                    continue

                articles = decompress(response).decode("utf-8").split("\n")
                print("Number of articles fetched - %s. In process %s" %(len(articles), os.getpid()))
                for article in articles:
                    if not len(article):
                        continue

                    try:
                        ar = yaml.safe_load(article)
                    except yaml.reader.ReaderError:
                        continue
                    if ar.get("lang") == "en":
                        for news_source in news_sources:
                            if news_source in ar.get("url"):
                                shared_list.append({"url": ar.get("url"), "sentiment": ar.get("score")})
                                print("Total count %s. Last found %s in process %s" %(len(shared_list), news_source, os.getpid()))
            
            urls = []


if __name__ == "__main__":
    cpu_count = multiprocessing.cpu_count()
 
    manager = multiprocessing.Manager()
    shared_list = manager.list()
    queue = Queue(maxsize=multiprocessing.cpu_count()*2)


    p = Process(target=producer, args=(queue, shared_list))
    p.start()

    consumers = []
    for i in range(multiprocessing.cpu_count()):
        c = Process(target=consumer, args=(queue, shared_list))
        c.start()
        consumers.append(c)
    
    p.join()
    for c in consumers:
        c.join()
    
    rows = [(i["url"], i["sentiment"]) for i in shared_list]
    
    print(f"Writing CSV at {os.getcwd()}")
    with open(os.getcwd()+"/news_articles.csv", "w+", newline="\n") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(rows)