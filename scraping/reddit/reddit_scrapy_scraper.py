import time
import traceback
import datetime as dt
from scraping.reddit.model import RedditScrapyContent
from scraping.scraper import ScrapeConfig, Scraper
import bittensor as bt
from scraping.reddit.utils import normalize_label, normalize_permalink
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

        parsed_contents = [self._best_effort_parse_comment(content, subreddit_name) for content in data ]
        print("parsed_contents", parsed_contents)

        return [RedditScrapyContent.to_data_entity(content) for content in parsed_contents if content is not None]


    def _best_effort_parse_comment(self, data, subreddit) -> RedditScrapyContent:
        """Performs a best effort parsing of a Reddit comment into a RedditContent

        Any errors are logged and ignored."""
        content = None

        try:
            content = RedditScrapyContent(
                id=data["id"],
                url="https://www.reddit.com" + normalize_permalink(data["url"]),
                text= data["text"],
                likes=data["likes"],
                datatype=data["datatype"],
                user_id=data["user_id"],
                username=data["username"],
                community=subreddit,
                created_at=data["timestamp"],
                title=data["title"],
                num_comments = data["num_comments"],
            )
            print("content_in_best_parse: ", content)

        except Exception as e:
            print('error_in_parse', e, traceback.format_exc())

        return content