import threading
from common import utils
from common.data import TimeBucket
from storage.miner.miner_storage import MinerStorage
from typing import List
import bittensor as bt
import datetime as dt
from pymongo import MongoClient


class MongodbMinerScrapyStorage(MinerStorage):
    """MongoDB backed MinerStorage"""

    def __init__(
        self,
        mongodb_uri="mongodb://localhost:27017/",
        database="mongo_miner_storage",
        max_database_size_gb_hint=250,
    ):
        bt.logging.info(
            f"Mongo config: {mongodb_uri}, {database}, {max_database_size_gb_hint}"
        )
        self.database_max_content_size_bytes = utils.gb_to_bytes(
            max_database_size_gb_hint
        )
        try:
            self.mongodb_uri = mongodb_uri
            self.mongodb_database = database
            self.client = MongoClient(self.mongodb_uri)
            self.db = self.client[self.mongodb_database]
            meta_database = "bittensor_reddit_miner_meta"
            self.meta_db = self.client[meta_database]
            bt.logging.success(f"Mongo database connected.")

        except Exception as e:
            bt.logging.error("Error in mongodb creation: ", e)

        # Lock to avoid concurrency issues on clearing space when full
        self.clearing_space_lock = threading.Lock()

    def store_data_entities(self, data_entities):
        """Stores any number of DataEntities, making space if necessary."""

        with self.clearing_space_lock:
            # Insert or update data entities in the DataEntity collection.
            print("In_inserting_data: ", len(data_entities))

            for data_entity in data_entities:
                # print("In_inserting_data: ", data_entity)

                self.db.DataEntity.insert_one(
                    {
                        "id": data_entity["id"],
                        "url": data_entity["url"],
                        "text": data_entity["text"],
                        "likes": data_entity["likes"],
                        "datatype": data_entity["datatype"],
                        "user_id": data_entity["user_id"],
                        "username": data_entity["username"],
                        "timestamp": data_entity["timestamp"],
                        "num_comments": data_entity["num_comments"],
                        "title": data_entity["title"]
                    }
                )

    def list_data_entities_in_data_entity_bucket(self):
        """Lists from storage all DataEntities matching the provided DataEntityBucket."""
        pass

    def get_compressed_index(self):
        """Gets the compressed MinedIndex, which is a summary of all of the DataEntities that this MinerStorage is currently serving."""
        pass

    def check_labels(self, miner_id):
        self.miner_labels_db = self.meta_db['MinerLabels']
        miner_labels = list(self.miner_labels_db.find({'miner_id': miner_id}))
        if not miner_labels:
            print("In if to find label with id='None")
            miner_labels = list(self.miner_labels_db.find({'miner_id': None}))
            
        return miner_labels
    
    def store_miner_label(self, miner_data):
        self.miner_labels_db = self.meta_db['MinerLabels']
        query = {'miner_label': miner_data["miner_label"]}
        update_data = {'$set': {'miner_id': miner_data["miner_id"]}}
        result = self.miner_labels_db.update_one(query, update_data, upsert=True)
        return result

    def remove_miner_id(self, pod_name):
        self.miner_labels_db = self.meta_db['MinerLabels']
        result = self.miner_labels_db.update_one({"miner_id": pod_name}, {"$set": {"miner_id": None}})
        return result
