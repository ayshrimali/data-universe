
from reddit_scraper import settings
from reddit_scraper.base_pipeline import BaseMongoPipeline


class RedditScraperPipeline:
    def process_item(self, item, spider):
        return item


class RedditPostPipeline(BaseMongoPipeline):
    """
    Pipeline for processing mined data.

    Inherits from BaseMongoPipeline and specifies the collection name and index field for product index data.
    """

    def __init__(self, db=None):
        super().__init__(coll_name=settings.REDDIT_POST_COLLECTION_NAME, index_field='url', db=db)
