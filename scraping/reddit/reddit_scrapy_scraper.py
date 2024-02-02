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

    def spider_closed_callback(self, spider):
        print("Spider closed. Collected data:", self.scraped_data)

    def run_spider(self, subreddit):
        self.scraped_data = []  # Initialize scraped data list

        print("In run spider method", subreddit)
        setting = get_project_settings()
        process = CrawlerProcess(setting)

        # Connect the item_callback function to the spider_closed signal
        process.crawl(post_crawler.PostCrawlerSpider, subreddit=subreddit, days=30)
        crawler = process.crawlers.pop()
        crawler.signals.connect(self.item_callback, signal=signals.item_scraped)
        crawler.signals.connect(self.spider_closed_callback, signal=signals.spider_closed)
        process.crawlers.add(crawler)

        process.start(stop_after_crawl=False)

        return self.scraped_data

    def scrape(self, subreddit):
        subreddit_name = normalize_label(subreddit)
        print("subreddit", subreddit_name)

        data = self.run_spider(subreddit_name)

        print("data: ", data)

    async def scrape(self,scrape_config, subreddit):
        """Scrapes a batch of reddit posts/comments according to the scrape config."""
        # bt.logging.trace(
        #     f"Reddit custom scraper peforming scrape with config: {scrape_config}."
        # )

        # assert (
        #     not scrape_config.labels or len(scrape_config.labels) <= 1
        # ), "Can only scrape 1 subreddit at a time."

        # Strip the r/ from the config or use 'all' if no label is provided.
        subreddit_name = (
            normalize_label(subreddit)
        )

        # bt.logging.trace(
        #     f"Running custom Reddit scraper with search: {subreddit_name}."
        # )
        print("subreddit", subreddit_name)

        data = self.run_spider(subreddit_name)

        print("data: ",len(data), data[0])
        return data
