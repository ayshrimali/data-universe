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

    async def scrape(
        self, scrape_config: ScrapeConfig, subreddit, netuid
    ) -> List[DataEntity]:

        print("netuid: ", netuid)
        # Strip the r/ from the config or use 'all' if no label is provided.
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

        print("is_process_alive", p.is_alive())

        print("scraped_data: ", len(return_dict["scraped_data"]))

        if fetch_posts and not p.is_alive():
            if netuid == 13:
                parsed_contents = [
                    self._best_effort_parse_post(content, subreddit.value)
                    for content in return_dict["scraped_data"]
                ]
                return [
                    RedditScrapyContent.to_data_entity(content)
                    for content in parsed_contents
                    if content is not None
                ]
            if netuid == 3:
                data_entity = [
                    {
                        "id": post_data["id"],
                        "url": post_data["url"],
                        "text": post_data["text"],
                        "likes": post_data["score"],
                        "datatype": post_data["datatype"],
                        "timestamp": post_data["timestamp"],
                        "username": post_data["username"],
                        "community": subreddit.value,
                        "title": post_data["title"],
                        "num_comments": post_data["num_comments"],
                        "user_id": post_data["user_id"],
                    }
                    for post_data in return_dict["scraped_data"]
                ]
                return data_entity

        elif not fetch_posts and not p.is_alive():
            if netuid == 13:
                parsed_contents = [
                    self._best_effort_parse_comment(content, subreddit.value)
                    for content in return_dict["scraped_data"]
                ]
                return [
                    RedditScrapyContent.to_data_entity(content)
                    for content in parsed_contents
                    if content is not None
                ]
            if netuid == 3:
                data_entity = [
                    {
                        "id": comment_data["id"],
                        "url": comment_data["url"],
                        "text": comment_data["text"],
                        "likes": comment_data["score"],
                        "datatype": comment_data["datatype"],
                        "timestamp": comment_data["timestamp"],
                        "username": comment_data["username"],
                        "parent": comment_data["parent"],
                        "community": subreddit.value,
                    }
                    for comment_data in return_dict["scraped_data"]
                ]
                return data_entity

    def _best_effort_parse_comment(self, comment, subreddit) -> RedditScrapyContent:
        """Performs a best effort parsing of a Reddit data into a RedditScrapyContent
        Any errors are logged and ignored."""

        content = None
        try:
            content = RedditScrapyContent(
                id=comment["id"],
                url=comment["url"],
                text=comment["text"],
                likes=comment["score"],
                username=comment["username"],
                community=subreddit,
                created_at=comment["timestamp"],
                type="comment",
                parent=comment["parent"],
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
                likes=post["score"],
                datatype=post["datatype"],
                created_at=post["timestamp"],
                username=post["username"],
                community=subreddit,
                title=post["title"],
                num_comments=post["num_comments"],
                user_id=post["user_id"] if post["user_id"] else "[deleted]",
                type="post",
            )
            # print("content_in_best_parse_post: ", content)

        except Exception as e:
            print("error_in_post_parse", e, post)

        return content
