from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable

import scrapy
from dateutil import parser
from scrapy import Request

from reddit_scraper import settings, utils
from scraping.reddit.utils import get_custom_sort_input, get_time_input


class PostCrawlerSpider(scrapy.Spider):
    name = "post-crawler"
    url_template = (
        "https://www.reddit.com/svc/shreddit/community-more-posts/{sort_type}/"
    )

    custom_settings = {
        "ITEM_PIPELINES": {"reddit_scraper.pipelines.RedditPostPipeline": 400}
    }

    def __init__(self, scrape_config={}, subreddit="BitcoinBeginners", *args, **kwargs):
        super(PostCrawlerSpider, self).__init__(*args, **kwargs)
        self.subreddit = subreddit.value.removeprefix("r/")
        self.mined_data_list = []
        # Get the search terms for the reddit query.
        self.search_limit = scrape_config.entity_limit
        self.search_sort = get_custom_sort_input(scrape_config.date_range.end)
        self.search_time = get_time_input(scrape_config.date_range.end)

    def start_requests(self) -> Iterable[Request]:
        params = {
            "t": self.search_time,
            "name": self.subreddit,
            "feedLength": self.search_limit,
            "after": utils.encoded_base64_string("t3_19fa7bp"),
        }
        url = self.url_template.format(sort_type=self.search_sort)
        url = utils.join_url_params(url, params)

        yield Request(
            url=url, callback=self.parse, meta={"proxy": settings.PROXY_STRING}
        )

    def parse(self, response):
        posts_nodes = response.xpath("//shreddit-post")
        last_post_id = ""
        default_time = {
            "month": timedelta(days=30),
            "week": timedelta(days=7),
            "day": timedelta(days=1),
            "year": timedelta(days=365),
        }
        target_timestamp = datetime.now(timezone.utc) - default_time[self.search_time]

        for post_node in posts_nodes:
            mined_data = {
                "id": post_node.attrib.get("id"),
                "url": post_node.attrib.get("content-href")
                + post_node.attrib.get("id"),
                "text": utils.clean_text(
                    post_node.xpath(
                        './/div[@data-post-click-location="text-body"]//text()'
                    ).getall()
                ),
                "likes": response.xpath("//faceplate-number[1]/@number").get(),
                "datatype": post_node.attrib.get("post-type"),
                "timestamp": parser.parse(post_node.attrib.get("created-timestamp")),
                "username": post_node.attrib.get("author"),
                "community":  self.subreddit,
                "title": post_node.attrib.get("post-title"),
                "num_comments": post_node.attrib.get("comment-count"),
                "user_id": post_node.attrib.get("author-id"),
            }

            # Check if the post's timestamp is within the desired date range
            if mined_data["timestamp"] >= target_timestamp:
                self.mined_data_list.append(mined_data)
                last_post_id = mined_data["id"]
                yield mined_data

        # Only continue pagination if the last post is within the desired date range
        if (
            last_post_id
            and mined_data["timestamp"] >= target_timestamp
            and len(self.mined_data_list) < 100
        ):
            params = {
                "t": self.search_time,
                "name": self.subreddit,
                "feedLength": self.search_limit,
                "after": utils.encoded_base64_string(last_post_id),
            }

            url = self.url_template.format(sort_type=self.search_sort)
            url = utils.join_url_params(url, params)
            yield Request(
                url=url, callback=self.parse, meta={"proxy": settings.PROXY_STRING}
            )


# scrapy crawl post-crawler -a subreddit=Bitcoin -a days=30
