from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable

import scrapy
from dateutil import parser
from scrapy import Request

from reddit_scraper import settings, utils

PAGE_SIZE = 50


class SortType(Enum):
    NEW = "new"
    HOT = "hot"


class PostCrawlerSpider(scrapy.Spider):
    name = "post-crawler"
    url_template = "https://www.reddit.com/svc/shreddit/community-more-posts/{sort_type}/"

    custom_settings = {
        'ITEM_PIPELINES': {'reddit_scraper.pipelines.RedditPostPipeline': 400}
    }

    def __init__(self, subreddit="BitcoinBeginners", days=30, *args, **kwargs):
        super(PostCrawlerSpider, self).__init__()
        self.subreddit = subreddit
        self.days = int(days)

    def start_requests(self) -> Iterable[Request]:
        print("subreddit__in_spider", self.subreddit)
        params = {
            't': 'DAY',
            'name': self.subreddit,
            'feedLength': PAGE_SIZE,
            'after': utils.encoded_base64_string("t3_19fa7bp"),
        }
        url = self.url_template.format(sort_type=SortType.NEW.value)
        url = utils.join_url_params(url, params)

        yield Request(url=url, callback=self.parse, meta={"proxy": settings.PROXY_STRING})

    def parse(self, response):
        posts_nodes = response.xpath('//shreddit-post')
        last_post_id = ""
        target_timestamp = datetime.now(timezone.utc) - timedelta(days=self.days)

        for post_node in posts_nodes:
            mined_data = {
                "id": post_node.attrib.get("id"),
                "url": post_node.attrib.get("content-href") + post_node.attrib.get("id"),
                "text": utils.clean_text(post_node.xpath('.//div[@data-post-click-location="text-body"]//text()').getall()),
                "likes": post_node.attrib.get(""),
                "datatype": post_node.attrib.get("post-type"),
                "user_id": post_node.attrib.get("author-id"),
                "username": post_node.attrib.get("author"),
                "timestamp": parser.parse(post_node.attrib.get("created-timestamp")),
                "num_comments": post_node.attrib.get("comment-count"),
            }

            # Check if the post's timestamp is within the desired date range
            if mined_data['timestamp'] >= target_timestamp:
                yield mined_data
                last_post_id = mined_data['id']
            else:
                # Stop parsing further if a post is older than the target timestamp
                break

        # Only continue pagination if the last post is within the desired date range
        if last_post_id and mined_data['timestamp'] >= target_timestamp:
            params = {
                't': 'WEEK',
                'name': self.subreddit,
                'feedLength': PAGE_SIZE,
                'after': utils.encoded_base64_string(last_post_id),
            }

            url = self.url_template.format(sort_type=SortType.NEW.value)
            url = utils.join_url_params(url, params)
            yield Request(url=url, callback=self.parse, meta={"proxy": settings.PROXY_STRING})


# scrapy crawl post-crawler -a subreddit=Bitcoin -a days=30
