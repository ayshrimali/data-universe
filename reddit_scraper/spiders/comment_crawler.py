from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable

import scrapy
from dateutil import parser
from scrapy import Request

from reddit_scraper import settings, utils
from scraping.reddit.utils import get_custom_sort_input, get_time_input


class CommentCrawlerSpider(scrapy.Spider):
    name = "comment-crawler"
    url_template = "https://www.reddit.com/svc/shreddit/community-more-posts/{sort_type}/"
    comment_headers = {
        'authority': 'www.reddit.com',
        'accept': 'text/vnd.reddit.partial+html, text/html;q=0.9',
        'accept-language': 'en-US,en;q=0.9,bn;q=0.8',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://www.reddit.com',
        'referer': 'https://www.reddit.com/r/CryptoCurrency/comments/1acfaln/for_the_first_time_since_february_2021_gbtc/',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }

    custom_settings = {
        'ITEM_PIPELINES': {'reddit_scraper.pipelines.RedditPostPipeline': 400}
    }

    def __init__(self,scrape_config={}, subreddit="BitcoinBeginners", days=30, *args, **kwargs):
        super(CommentCrawlerSpider, self).__init__(*args, **kwargs)
        self.subreddit = subreddit.value.removeprefix("r/")
        self.mined_data_list = []

        # Get the search terms for the reddit query.
        self.search_limit = scrape_config.entity_limit
        self.search_sort = get_custom_sort_input(scrape_config.date_range.end)
        self.search_time = get_time_input(scrape_config.date_range.end)

    def start_requests(self) -> Iterable[Request]:
        params = {
            't': self.search_time,
            'name': self.subreddit,
            'feedLength': self.search_limit,
            'after': utils.encoded_base64_string("t3_19fa7bp"),
        }
        url = self.url_template.format(sort_type=self.search_sort)
        url = utils.join_url_params(url, params)

        yield Request(url=url, callback=self.parse, meta={"proxy": settings.PROXY_STRING})

    def parse_comment(self, response):
        comment_nodes = response.xpath('//shreddit-comment')
        self.logger.info(f"comment count for the post: {len(comment_nodes)} - {response.url}")
        for comment in comment_nodes:
            time_stamp = None
            try:
                time_stamp = parser.parse(comment.xpath('.//faceplate-timeago')[0].attrib.get('ts'))
            except:
                pass
            comment_data = {
                "id": comment.attrib.get("thingid"),
                "url": "https://www.reddit.com" + comment.attrib.get("permalink"),
                "text": utils.clean_text(
                    comment.xpath(
                        './/div[contains(@id, "post-rtjson-content")]//text()'
                    ).getall()
                ),
                "datatype": comment.attrib.get("content-type"),
                "timestamp": time_stamp,
                "username": comment.attrib.get("author"),
                "parent": comment.attrib.get("postid"),
                "community":  self.subreddit,
                "depth": comment.attrib.get("depth"),
                "reload-url": comment.attrib.get("reload-url"),
                "score": comment.attrib.get("score"),
                "parent-id": comment.attrib.get("parentid"),
            }
            yield comment_data

    def parse(self, response):
        posts_nodes = response.xpath('//shreddit-post')
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
                "url": post_node.attrib.get("content-href") + post_node.attrib.get("id"),
                "text": utils.clean_text(
                    post_node.xpath(
                        './/div[@data-post-click-location="text-body"]//text()'
                    ).getall()
                ),
                "likes": response.xpath("//faceplate-number[1]/@number").get(),
                "datatype": post_node.attrib.get("post-type"),
                "user_id": post_node.attrib.get("author-id"),
                "username": post_node.attrib.get("author"),
                "timestamp": parser.parse(post_node.attrib.get("created-timestamp")),
                "num_comments": post_node.attrib.get("comment-count"),
                "title": post_node.attrib.get("post-title"),
                "type": "post",
                "subreddit-prefixed-name": post_node.attrib.get(
                    "subreddit-prefixed-name"
                ),
            }

            if mined_data["timestamp"] >= target_timestamp:
                # yield mined_data
                self.mined_data_list.append(mined_data)
                last_post_id = mined_data['id']
                comment_url = f'https://www.reddit.com/svc/shreddit/more-comments/{self.subreddit}/{mined_data["id"]}'

                # now follow the comments
                params = {
                    "sort": "TOP",
                    "top-level": "1",
                }
                comment_url = utils.join_url_params(comment_url, params)

                self.comment_headers.update({"referer": mined_data["url"]})
                yield Request(url=comment_url,
                            method="POST",
                            headers=self.comment_headers,
                            callback=self.parse_comment,
                            meta={"proxy": settings.PROXY_STRING})

        # Only continue pagination if the last post is within the desired date range
        if (
            last_post_id
            and mined_data["timestamp"] >= target_timestamp
            and len(self.mined_data_list) < 100
        ):
            params = {
                't': self.search_time,
                'name': self.subreddit,
                'feedLength': self.search_limit,
                'after': utils.encoded_base64_string(last_post_id),
            }

            url = self.url_template.format(sort_type=self.search_sort)
            url = utils.join_url_params(url, params)
            yield Request(url=url, callback=self.parse, meta={"proxy": settings.PROXY_STRING})
