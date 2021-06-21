import signal
import asyncio
import aiohttp
import scraper_functions
from multiprocessing import Process
from inspect import getmembers, isfunction
from helpers import validate_news_sources


class ArticleScraper(Process):
    '''
    Responsible for scraping news articles
    Article URLs are scraped by GDELTScraper
    Scraper functions for each domain are implemented in scraper_functions python file
    '''
    def __init__(self, news_sources, scraper_queue, writer_queue):
        super(ArticleScraper, self).__init__()
        validate_news_sources(news_sources)
        self.scraper_queue = scraper_queue
        self.writer_queue = writer_queue
        self.news_sources = self._convert_func_names_to_func_references(news_sources)
    
    def _convert_func_names_to_func_references(self, news_sources):
        module_functions = {fn_name: fn_ref for fn_name,
                        fn_ref in getmembers(scraper_functions, isfunction)}
        
        for news_source in news_sources:
            try:
                news_source["function_name"] = module_functions[news_source["function_name"]]
            except KeyError:
                # Should not happen
                continue
        return news_sources

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

    async def _consumer(self):
        async with aiohttp.ClientSession() as session:
            while True:
                data = self.scraper_queue.get()

                if data is None:
                    print("Goodbye from article scraper")
                    self.writer_queue.put(None)
                    break
                
                try:
                    url = data["url"]
                except KeyError:
                    print("URL to scrape not found")
                    continue
                
                print(f"Scraping url: {url}")
                article_text = ""

                for news_source in self.news_sources:
                    if news_source["source"] == data["source"]:
                        try:
                            article_text = await news_source["function_name"](url, session)
                        except asyncio.CancelledError:
                            self.writer_queue.put(None)
                            break

                if not article_text:
                    print(f"Implementation for {data['source']} not found in scraper functions.")
                    continue
                
                data["article_text"] = article_text
                data.pop("source")
                self.writer_queue.put(data)
    
    def run(self):
        loop = asyncio.new_event_loop()
        for signame in ['SIGTERM', 'SIGINT', 'SIGHUP']:
                    loop.add_signal_handler(getattr(signal, signame), lambda: asyncio.ensure_future(
                        self.signal_handler(signame, loop)))

        loop.run_until_complete(self._consumer())
    
