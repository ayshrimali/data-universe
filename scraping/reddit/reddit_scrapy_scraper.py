import random
import datetime as dt
from typing import List
from common.data import DataEntity
from scraping.reddit.model import RedditScrapyContent
from scraping.scraper import ScrapeConfig, Scraper
import bittensor as bt
from scraping.reddit.utils import normalize_label
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy.utils.project import get_project_settings
from reddit_scraper.spiders import comment_crawler, post_crawler
from scrapy import signals
from multiprocessing import Process, Manager


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

    def crawler_runner(self, scrape_config, subreddit, fetch_posts, return_dict):
        try:
            self.scraped_data = []
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
                    subreddit=subreddit,
                )
            process.start()
        except Exception as e:
            print("Error_in_crawler_runner: ", e)

        print("data_len_in_crawler", len(self.scraped_data))

        return_dict["scraped_data"] = self.scraped_data

    async def scrape(self, scrape_config: ScrapeConfig,  subreddit, netuid) -> List[DataEntity]:

        print("netuid: ", netuid)
        # Strip the r/ from the config or use 'all' if no label is provided.
        subreddit_name = normalize_label(subreddit)
        fetch_posts = bool(random.getrandbits(1))
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
            if netuid == 13:
                return [
                    RedditScrapyContent.to_data_entity(content)
                    for content in parsed_contents
                    if content is not None
                ]
            if netuid == 3:
                return return_dict["scraped_data"]

        elif not fetch_posts and not p.is_alive():
            parsed_contents = [
                self._best_effort_parse_comment(content, subreddit_name)
                for content in return_dict["scraped_data"]
            ]
            if netuid == 13:
                return [
                    RedditScrapyContent.to_data_entity(content)
                    for content in parsed_contents
                    if content is not None
                ]
            if netuid == 3:
                return return_dict["scraped_data"]


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
