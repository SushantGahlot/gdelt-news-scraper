import os
import yaml
import signal
import urllib
import aiohttp
import asyncio
import multiprocessing
from helpers import validate_news_sources
from gzip import decompress
from multiprocessing import Queue
from multiprocessing.context import Process


class GDELTScraper(Process):
    '''
    Responsible for scraping GDELT entity data to scrape news sources and associated sentiment. 
    https://blog.gdeltproject.org/announcing-the-global-entity-graph-geg-and-a-new-11-billion-entity-dataset/
    
    The news sources that GDELT scrapes can be found here 
    http://data.gdeltproject.org/supportingdatasets/DOMAINSBYCOUNTRY-ENGLISH.TXT
    '''

    def __init__(self, scraper_queue, news_sources, gdelt_url=None):
        super(GDELTScraper, self).__init__()
        validate_news_sources(news_sources)
        self.scraper_queue = scraper_queue
        self.cpu_count = multiprocessing.cpu_count()
        self.internal_queue = Queue(maxsize=self.cpu_count*2)
        self.news_sources = [news_source["source"] for news_source in news_sources]
        self.gdelt_url = "http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/MASTERFILELIST.TXT" if not gdelt_url else gdelt_url

    
    def _producer(self):
        '''Get urls from master file and put them in queue for _consumer'''
        resp = urllib.request.urlopen(self.gdelt_url).read().decode("utf-8")
        urls = reversed([i for i in resp.split("\n") if len(i)])

        for url in urls:
            self.internal_queue.put(url)

    async def _get_data(self, url, session):
        print("Fetching data from %s in process - %s" % (url, os.getpid()))
        try:
            r = await session.request("GET", url=url)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return
        return await r.read()

    async def _create_tasks(self, urls):
        async with aiohttp.ClientSession() as session:
            tasks = [self._get_data(dt, session) for dt in urls]
            try:
                data = await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                print("Tasks Cancelled")
                return []
        return data

    @staticmethod
    async def signal_handler(signal, loop):
        for task in asyncio.all_tasks(loop=loop):
            # cancel all tasks other than this signal_handler
            if task is not asyncio.current_task(loop):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    print("Task cancelled")
        loop.stop()
        return []

    def gdelt_extractor(self, ar):
        '''
        Extract url and sentiment from urls present in gzip files.
        Visit the link in class docstring to see what all can be extracted.
        '''
        if ar.get("lang") == "en":
            for news_source in self.news_sources:
                if news_source in ar.get("url"):
                    print("Putting in queue")
                    self.scraper_queue.put(
                        {"url": ar.get("url"), "sentiment": ar.get("score"), "source": news_source})

    def _consumer(self):
        '''
        Get urls from the _producer, make requests on those urls.
        Each url will be a gzip file.
        In each gzip file, there will be number of article urls and sentiment associated with those urls.
        '''
        urls = []

        while True:
            url = self.internal_queue.get()

            if url is None:
                print("Goodbye from process %s" % os.getpid())
                break

            urls.append(url)

            if len(urls) >= 10:
                loop = asyncio.new_event_loop()
                for signame in ['SIGTERM', 'SIGINT', 'SIGHUP']:
                    loop.add_signal_handler(getattr(signal, signame), lambda: asyncio.ensure_future(
                        self.signal_handler(signame, loop)))

                results = loop.run_until_complete(self._create_tasks(urls))

                print("Fetched all data. Got %s results. In process %s" %
                      (len(results), os.getpid()))

                for response in results:
                    if not len(response):
                        continue

                    articles = decompress(response).decode("utf-8").split("\n")
                    print("Number of articles fetched - %s. In process %s" %
                          (len(articles), os.getpid()))
                    for article in articles:
                        if not len(article):
                            continue

                        try:
                            ar = yaml.safe_load(article)
                        except yaml.reader.ReaderError:
                            continue

                        self.gdelt_extractor(ar)

                urls = []

    def run(self):
        p = Process(target=self._producer)
        p.start()
        consumers = []
        for _ in range(self.cpu_count):
            c = Process(target=self._consumer)
            c.start()
            consumers.append(c)

        try:
            p.join()
            for c in consumers:
                c.join()
        except KeyboardInterrupt:
            print("Killing processes. Please wait for all the processes to stop.")
            # Can terminate this, no child processes of this one.
            p.terminate()
            for _ in consumers:
                self.internal_queue.put(None)
            self.scraper_queue.put(None)
