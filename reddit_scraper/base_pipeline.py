import logging

from pymongo import MongoClient, ReplaceOne
from pymongo.errors import AutoReconnect
from retry import retry

from reddit_scraper import settings

logger = logging.getLogger(__name__)


class BaseMongoPipeline:
    """
    Base class for MongoDB pipelines with dynamic index creation.

    Attributes:
        database_url (str): Connection string for the MongoDB database.
        db_name (str): Name of the MongoDB database.
        coll_name (str): Name of the MongoDB collection.
        index_field (str): Field name on which the unique index is created.
        db (MongoClient, optional): MongoClient instance. If not provided, a new connection is established.
        batch_size (int): Size of the batch for bulk processing.
        batch (list): Temporary storage for the current batch of items.

    Methods:
        open_spider(spider): Initializes the spider, sets up MongoDB connection and creates an index on the specified field.
        close_spider(spider): Closes the spider and processes any remaining items in the batch. Closes the MongoDB connection if it was created internally.
        bulk_process_items(items, spider): Processes items in bulk. This method should be implemented in subclasses.
        process_item(item, spider): Adds an item to the current batch for processing. When batch size is reached, it triggers bulk processing.

    """

    def __init__(self, coll_name, index_field, db=None, batch_size=settings.DB_BATCH_SIZE):
        self.database_url = settings.MONGO_CONNECTION_STRING
        self.db_name = settings.MONGO_DB_NAME
        self.coll_name = coll_name
        self.index_field = index_field
        self.db = db
        self.batch_size = batch_size
        self.batch = []

    def open_spider(self, spider):
        if self.db:
            self.mongo_client = None
        else:
            self.mongo_client = MongoClient(self.database_url)
            self.db = self.mongo_client[self.db_name]

        self.collection = self.db[self.coll_name]
        if self.index_field != "_id":
            self.collection.create_index([(self.index_field, 1)], unique=True)

    def close_spider(self, spider):
        if self.batch:
            self.bulk_process_items(self.batch, spider)
            self.batch = []

        if self.mongo_client:
            self.mongo_client.close()

    @retry(AutoReconnect, tries=4, delay=20)
    def bulk_process_items(self, items, spider):
        logger.info(f"processing {len(items)} items...")
        ops = [ReplaceOne({self.index_field: item[self.index_field]}, item, upsert=True)
               for item in items if item.get(self.index_field)]

        if ops:
            result = self.collection.bulk_write(ops, ordered=False)
            logger.info({k: v for k, v in result.bulk_api_result.items()
                         if k.startswith("write") or k.startswith("n")})
        else:
            logger.info("all items are invalid, nothing to process")

    def process_item(self, item, spider):
        self.batch.append(item)

        if len(self.batch) >= self.batch_size:
            self.bulk_process_items(self.batch, spider)
            self.batch = []

        return item
