import time
from scraping.reddit import model
from scraping.scraper import ScrapeConfig, Scraper
import bittensor as bt
from scraping.reddit.utils import normalize_label
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from reddit_scraper.spiders import post_crawler
from scrapy import cmdline
from scrapy import signals
from dotenv import load_dotenv

load_dotenv()


class RedditScrapyScraper(Scraper):
    """
    Scrapes Reddit data using a scrapy.
    """

    async def validate(self):
        """Validate the correctness of a DataEntity by URI."""
        pass

    def item_callback(self, item, response, spider):
        print("item, response", item, response)
        self.scraped_data.append(item)

    def run_spider(self, subreddit):
        self.scraped_data = []

        print("In run spider method", subreddit)
        setting = get_project_settings()
        process = CrawlerProcess(setting)

        # Connect the item_callback function to the spider_closed signal
        process.crawl(post_crawler.PostCrawlerSpider, subreddit=subreddit, days=30)
        crawler = process.crawlers.pop()
        crawler.signals.connect(self.item_callback, signal=signals.item_scraped)
        process.crawlers.add(crawler)

        # process.start(stop_after_crawl=False)
        process.start()

        return self.scraped_data

    def scrape(self, scrape_config, subreddit):
        # Strip the r/ from the config or use 'all' if no label is provided.
        subreddit_name = normalize_label(subreddit)

        print("subreddit", subreddit_name)

        data = self.run_spider(subreddit_name)

        print("data: ",len(data))
        return data
