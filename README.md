# News Articles Scraper Pipeline

## Introduction

GDELT scraper to scrape news articles and their sentiment scores to build news sentiment analysis models.

1. Scrape [GDELT](https://www.gdeltproject.org) data to get news article urls and their sentiment scores 
2. Scrape news articles from their urls.
3. **Pipeline implementation**. It means the scraper will  
    - First, scrape article urls and their sentiments 
    - Second, scrape the news article from the url
    - Third, write them to the csv
    
    without user intervention.

---

## How to roll your own scrapers

1. Make sure that news source exists in the [sources]() that GDELT scrapes.
2. Implement a scraper function for that source. Make sure you can scrape the news article by calling this (async)function
3. Add the function that you implemented in `scraper_functions.py`
4. Add the URL and the function name in `sources` mapping in `main.py`
5. Run `main.py`
7. It will write to a file called `articles.csv` in current working directory
6. Profit

---

### ToDos

- [ ] Replace prints with loggers
- [ ] Implement rotating IPs for scraper functions

---

### Tech stuff
- Uses multi processing, threading and asyncio. Will try to run all all the cores.
- `main.py` is the entry point for the app. 