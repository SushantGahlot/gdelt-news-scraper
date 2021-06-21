from multiprocessing import Queue
from gdelt_scraper import GDELTScraper
from article_writer import ArticleWriter
from article_scraper import ArticleScraper


def main():
    sources = [
        {
            "source": "www.theguardian.com",
            "function_name": "theguardian"
        },
        {
            "source": "://news.yahoo.com",
            "function_name": "yahoo"
        },
        {
            "source": "://www.nytimes.com",
            "function_name": "nytimes"
        }
    ]

    scraper_queue = Queue(maxsize=500)
    writer_queue = Queue()

    gdelt_scraper = GDELTScraper(
        scraper_queue=scraper_queue, news_sources=sources)
    article_scraper = ArticleScraper(
        news_sources=sources, scraper_queue=scraper_queue, writer_queue=writer_queue)
    article_writer = ArticleWriter(writer_queue)

    try:
        gdelt_scraper.start()
        article_scraper.start()
        article_writer.start()
    except KeyboardInterrupt:
        gdelt_scraper.terminate()
        article_writer.terminate()
        article_scraper.terminate()

    gdelt_scraper.join()
    article_scraper.join()
    article_writer.join()


if __name__ == "__main__":
    main()
