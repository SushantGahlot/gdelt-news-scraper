import os
import yaml
import signal
import urllib
import aiohttp
import asyncio
import multiprocessing
from gzip import decompress
from multiprocessing import Queue
from multiprocessing.context import Process


class GDELT:
    def __init__(self, scraper_queue, news_sources=None, gdelt_url=None):
        self.scraper_queue = scraper_queue
        self.cpu_count = multiprocessing.cpu_count()
        self.internal_queue = Queue(maxsize=self.cpu_count*2)
        self.news_sources = [
            "www.theguardian.com",
            "://news.yahoo.com",
            "://www.nytimes.com",
            "://thehindu.com"
        ] if not news_sources else news_sources
        self.gdelt_url = "http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/MASTERFILELIST.TXT" if not gdelt_url else gdelt_url
        self.exit = multiprocessing.Event()

    def _producer(self):
        resp = urllib.request.urlopen(self.gdelt_url).read().decode("utf-8")
        urls = reversed([i for i in resp.split("\n") if len(i)])
        urls = list(urls)
        urls = urls[int(len(urls)/2):]

        while not self.exit.is_set():
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

    def _consumer(self):
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
                        if ar.get("lang") == "en":
                            for news_source in self.news_sources:
                                if news_source in ar.get("url"):
                                    self.scraper_queue.put(
                                        {"url": ar.get("url"), "sentiment": ar.get("score")})

                urls = []

    def start(self):
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
            print("Killing processes. Please wait for all processes to stop.")
            # Can terminate this, no child processes of this one.
            p.terminate()
            for _ in consumers:
                self.internal_queue.put(None)
