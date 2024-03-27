from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable

import scrapy
from dateutil import parser
from scrapy import Request

from reddit_scraper import settings, utils

PAGE_SIZE = 5


class SortType(Enum):
    NEW = "new"
    HOT = "hot"


class PostCrawlerSpider(scrapy.Spider):
    name = "post-crawler"
    url_template = "https://www.reddit.com/svc/shreddit/community-more-posts/{sort_type}/"

    custom_settings = {
        'ITEM_PIPELINES': {'reddit_scraper.pipelines.RedditPostPipeline': 400}
    }

    def __init__(self, subreddit="CryptoMoonShots", days=1, *args, **kwargs):
        super(PostCrawlerSpider, self).__init__()
        self.subreddit = subreddit
        self.days = int(days)

    def start_requests(self) -> Iterable[Request]:
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
                "content-href": post_node.attrib.get("content-href"),
                # "view-context": post_node.attrib.get("view-context"),
                # "comment-count": post_node.attrib.get("comment-count"),
                # "feedindex": post_node.attrib.get("feedindex"),
                "created-timestamp": parser.parse(post_node.attrib.get("created-timestamp")),
                "id": post_node.attrib.get("id"),
                # "post-title": post_node.attrib.get("post-title"),
                # "post-type": post_node.attrib.get("post-type"),
                # "score": post_node.attrib.get("score"),
                # "subreddit-id": post_node.attrib.get("subreddit-id"),
                "subreddit-prefixed-name": post_node.attrib.get("subreddit-prefixed-name"),
                # "author-id": post_node.attrib.get("author-id"),
                # "author": post_node.attrib.get("author"),
                "text": utils.clean_text(post_node.xpath('.//div[@data-post-click-location="text-body"]//text()').getall()),
                "body": ";;;".join(post_node.xpath(
                        './/div[@data-post-click-location="text-body"]'
                    ).getall()),
                "url": post_node.attrib.get("content-href") + post_node.attrib.get("id")
            }

            # Check if the post's timestamp is within the desired date range
            if mined_data['created-timestamp'] >= target_timestamp:
                yield mined_data
                last_post_id = mined_data['id']
            else:
                # Stop parsing further if a post is older than the target timestamp
                break

        # Only continue pagination if the last post is within the desired date range
        # if last_post_id and mined_data['created-timestamp'] >= target_timestamp:
        #     params = {
        #         't': 'WEEK',
        #         'name': self.subreddit,
        #         'feedLength': PAGE_SIZE,
        #         'after': utils.encoded_base64_string(last_post_id),
        #     }

        #     url = self.url_template.format(sort_type=SortType.NEW.value)
        #     url = utils.join_url_params(url, params)
        #     yield Request(url=url, callback=self.parse, meta={"proxy": settings.PROXY_STRING})
