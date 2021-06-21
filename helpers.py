import scraper_functions
from inspect import getmembers, isfunction


class EmptyNewsSources(Exception):
    pass


class EmptyScraperSource(Exception):
    pass


class EmptyScraperFunction(Exception):
    pass


class ScraperFunctionNotImplemented(Exception):
    pass


def validate_news_sources(news_sources):
    module_functions = {fn_name: fn_ref for fn_name,
                        fn_ref in getmembers(scraper_functions, isfunction)}

    if not len(news_sources):
        raise EmptyNewsSources("News sources can not be empty")

    for news_source in news_sources:
        source = news_source.get("source")
        if not source:
            raise EmptyScraperSource(
                f"Scraper source url not found for {news_source.get('function_name')}")
        
        function_name = news_source.get("function_name")
        if not function_name:
            raise EmptyScraperFunction(
                f"Scraper function not found for {news_source.get('source')}")

        if function_name not in module_functions:
            raise ScraperFunctionNotImplemented(
                f"Please check if function with name {function_name} exists in scraper_functions module")
    
