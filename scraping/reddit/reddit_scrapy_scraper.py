import asyncio
import time
import traceback
import datetime as dt
from common.data import DataLabel
from scraping.reddit.model import RedditScrapyContent
from scraping.scraper import ScrapeConfig, Scraper
import bittensor as bt
from scraping.reddit.utils import normalize_label, normalize_permalink
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
from scrapy.signalmanager import dispatcher
from twisted.internet import reactor
from scrapy.utils.log import configure_logging
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
        self.scraped_data.append(item)

    # def spider_ended(self, reason):
    #     print("Spider closed: ",post_crawler.PostCrawlerSpider.name, reason)

    # def run_spider(self, subreddit):
    #     self.scraped_data = []

    #     setting = get_project_settings()
    #     process = CrawlerProcess(setting)

    #     # Connect the item_callback function to the spider_closed signal
    #     process.crawl(post_crawler.PostCrawlerSpider, subreddit=subreddit, days=30)
    #     crawler = process.crawlers.pop()
    #     crawler.signals.connect(self.item_callback, signal=signals.item_scraped)
    #     crawler.signals.connect(self.spider_ended, signal=signals.spider_closed)
    #     process.crawlers.add(crawler)
    #     process.start(stop_after_crawl=False)
    #     process.stop()

    #     return process

    def crawler_runner(self, subreddit):
        self.scraped_data = []

        configure_logging()    
        runner = CrawlerRunner()

        dispatcher.connect(self.item_callback, signal=signals.item_scraped)
        d = runner.crawl(post_crawler.PostCrawlerSpider, subreddit=subreddit, days=30)
        d.addBoth(lambda _: reactor.stop())
        reactor.run()
        reactor.callFromThread(reactor.stop)
        return

    async def scrape(self, scrape_config, subreddit):
        # Strip the r/ from the config or use 'all' if no label is provided.
        subreddit_name = normalize_label(subreddit)
        self.crawler_runner(subreddit_name)
        
        # print("data: ",len(self.scraped_data))

        parsed_contents = [self._best_effort_parse_data(content, subreddit_name) for content in self.scraped_data ]
        print("parsed_contents", len(parsed_contents))

        return [RedditScrapyContent.to_data_entity(content) for content in parsed_contents if content is not None]
   

    def _best_effort_parse_data(self, data, subreddit) -> RedditScrapyContent:
        """Performs a best effort parsing of a Reddit data into a RedditScrapyContent
        Any errors are logged and ignored."""

        content = None
        try:
            content = RedditScrapyContent(
                id=data["id"],
                url="https://www.reddit.com" + normalize_permalink(data["url"]),
                text= data["text"],
                likes=data["likes"],
                datatype=data["datatype"],
                user_id=data["user_id"] if data["user_id"] else '[deleted]',
                username=data["username"],
                community=subreddit,
                created_at=data["timestamp"],
                title=data["title"],
                num_comments = data["num_comments"],
            )
            # print("content_in_best_parse: ", content)

        except Exception as e:
            print('error_in_parse', e, data)

        return content

# To test it manually
if __name__ == '__main__':
    # reddit_scraper = RedditScrapyScraper()
    # asyncio.run(reddit_scraper.run())
    i = 0
    while i < 2:

        async def test_scrape():
            reddit_scraper = RedditScrapyScraper()
            await reddit_scraper.scrape({}, DataLabel(value = "r/BitcoinBeginners"))
        asyncio.run(test_scrape())

        i += 1