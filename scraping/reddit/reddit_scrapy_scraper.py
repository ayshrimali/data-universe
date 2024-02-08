import asyncio
import random
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
from reddit_scraper.spiders import comment_crawler, post_crawler
from scrapy import cmdline
from scrapy import signals
from dotenv import load_dotenv
from multiprocessing import Process, Manager

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
        print("data_stored_in_scraped_data: ", len(self.scraped_data))

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

    def crawler_runner(self, scrape_config, subreddit, fetch_posts, return_dict):
        try:
            self.scraped_data = []
            # configure_logging()
            # runner = CrawlerRunner()
            process = CrawlerProcess(get_project_settings())

            dispatcher.connect(self.item_callback, signal=signals.item_scraped)

            if fetch_posts:
                process.crawl(
                    post_crawler.PostCrawlerSpider,
                    scrape_config=scrape_config,
                    subreddit=subreddit,
                )
            else:
                process.crawl(
                    comment_crawler.CommentCrawlerSpider,
                    scrape_config=scrape_config,
                    subreddit=subreddit.value,
                )
            process.start()

            # d.addBoth(lambda _: reactor.stop())
            # reactor.run()
            # reactor.callFromThread(reactor.stop)
        except Exception as e:
            print("Error_in_crawler_runner: ", e)
        print("data_len_in_crawler", len(self.scraped_data))

        return_dict["scraped_data"] = self.scraped_data

    def run(self, subreddit, fetch_posts, return_dict):
        asyncio.run(self.crawler_runner(subreddit, fetch_posts, return_dict))

    async def scrape(self, scrape_config, subreddit):
        # Strip the r/ from the config or use 'all' if no label is provided.
        subreddit_name = normalize_label(subreddit)
        # fetch_posts = bool(random.getrandbits(1))
        fetch_posts = True  ##Temporary scraping only post
        manager = Manager()
        return_dict = manager.dict()
        data = []

        p = Process(
            target=self.crawler_runner,
            args=[scrape_config, subreddit, fetch_posts, return_dict],
        )
        data.append(p)
        p.start()
        for proc in data:
            proc.join()

        print("is_process_alive", p.is_alive(), return_dict["scraped_data"])

        print("scraped_data: ", len(return_dict["scraped_data"]))

        if fetch_posts and not p.is_alive():
            parsed_contents = [
                self._best_effort_parse_post(content, subreddit_name)
                for content in return_dict["scraped_data"]
            ]
            return [
                RedditScrapyContent.to_data_entity(content)
                for content in parsed_contents
                if content is not None
            ]
        else:
            parsed_contents = [
                self._best_effort_parse_comment(content, subreddit_name)
                for content in self.scraped_data
            ]

    def _best_effort_parse_comment(self, comment, subreddit) -> RedditScrapyContent:
        """Performs a best effort parsing of a Reddit data into a RedditScrapyContent
        Any errors are logged and ignored."""

        content = None
        try:
            content = RedditScrapyContent(
                id=comment["id"],
                url=comment["url"],
                text=comment["text"],
                likes=comment["likes"],
                username=comment["username"],
                community=subreddit,
                created_at=comment["timestamp"],
                type=comment["type"],
            )
            # print("content_in_best_parse_comment: ", content)

        except Exception as e:
            print("error_in_comment_parse", e, comment)

        return content

    def _best_effort_parse_post(self, post, subreddit) -> RedditScrapyContent:
        """Performs a best effort parsing of a Reddit data into a RedditScrapyContent
        Any errors are logged and ignored."""

        content = None
        try:
            content = RedditScrapyContent(
                id=post["id"],
                url=post["url"],
                text=post["text"],
                likes=post["likes"],
                datatype=post["datatype"],
                user_id=post["user_id"] if post["user_id"] else "[deleted]",
                username=post["username"],
                community=subreddit,
                created_at=post["timestamp"],
                title=post["title"],
                type=post["type"],
                num_comments=post["num_comments"],
            )
            # print("content_in_best_parse_post: ", content)

        except Exception as e:
            print("error_in_post_parse", e, post)

        return content
