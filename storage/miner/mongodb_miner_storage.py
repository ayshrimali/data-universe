from collections import defaultdict
import threading
from common import constants, utils
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)
from storage.miner.miner_storage import MinerStorage
from typing import List
import bittensor as bt
import datetime as dt
from pymongo import MongoClient

class MongodbMinerStorage(MinerStorage):
    """MongoDB backed MinerStorage"""

    def __init__(
        self,
        mongodb_uri="mongodb://localhost:27017/",
        database="mongo_miner_storage",
        max_database_size_gb_hint=250,
    ):
        bt.logging.info(f"Mongo config: {mongodb_uri}, {database}, {max_database_size_gb_hint}")
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

    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""

        added_content_size = sum(data_entity.content_size_bytes for data_entity in data_entities)

        # If the total size of the store is larger than our maximum configured stored content size then except.
        if added_content_size > self.database_max_content_size_bytes:
            raise ValueError(
                f"Content size to store: {added_content_size} exceeds configured max: {self.database_max_content_size_bytes}"
            )

        with self.clearing_space_lock:
            # If we would exceed our maximum configured stored content size then clear space.
            current_content_size = self.db.DataEntity.estimated_document_count()

            if current_content_size + added_content_size > self.database_max_content_size_bytes:
                content_bytes_to_clear = (
                    self.database_max_content_size_bytes // 10
                    if self.database_max_content_size_bytes // 10 > added_content_size
                    else added_content_size
                )
                self.clear_content_from_oldest(content_bytes_to_clear)

            # Insert or update data entities in the DataEntity collection.
            for data_entity in data_entities:
                label = data_entity.label.value if data_entity.label else None
                time_bucket_id = TimeBucket.from_datetime(data_entity.datetime).id

                self.db.DataEntity.replace_one(
                    {"uri": data_entity.uri},
                    {
                        "uri": data_entity.uri,
                        "datetime": data_entity.datetime,
                        "timeBucketId": time_bucket_id,
                        "source": data_entity.source,
                        "label": label,
                        "content": data_entity.content,
                        "contentSizeBytes": data_entity.content_size_bytes,
                    },
                    upsert=True,
                )

    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucketId."""
        label = data_entity_bucket_id.label.value if data_entity_bucket_id.label else None

        query = {
            "timeBucketId": data_entity_bucket_id.time_bucket.id,
            "source": data_entity_bucket_id.source.value,
            "label": label,
        }

        data_entities = self.db.DataEntity.find(query)

        return [
            DataEntity(
                uri=data_entity["uri"],
                datetime=data_entity["datetime"],
                source=DataSource(data_entity["source"]),
                label=DataLabel(value=data_entity["label"]) if data_entity["label"] else None,
                content=data_entity["content"],
                content_size_bytes=data_entity["contentSizeBytes"],
            )
            for data_entity in data_entities
        ]

    def get_compressed_index(
        self,
        bucket_count_limit=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
    ) -> CompressedMinerIndex:
        """Gets the compressed MinerIndex, which is a summary of all DataEntities."""
        oldest_time_bucket_id = TimeBucket.from_datetime(
            dt.datetime.now()
            - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
        ).id

        pipeline = [
            {"$match": {"timeBucketId": {"$gte": oldest_time_bucket_id}}},
            {"$group": {"_id": {"timeBucketId": "$timeBucketId", "source": "$source", "label": "$label"}, "bucketSize": {"$sum": "$contentSizeBytes"}}},
            {"$sort": {"bucketSize": -1}},
            {"$limit": bucket_count_limit},
        ]

        result = list(self.db.DataEntity.aggregate(pipeline))

        buckets_by_source_by_label = defaultdict(dict)

        for entry in result:
            size = min(entry["bucketSize"], constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)

            label = entry["_id"]["label"]

            bucket = buckets_by_source_by_label[DataSource(entry["_id"]["source"])].get(
                label, CompressedEntityBucket(label=label)
            )
            bucket.sizes_bytes.append(size)
            bucket.time_bucket_ids.append(entry["_id"]["timeBucketId"])
            buckets_by_source_by_label[DataSource(entry["_id"]["source"])][label] = bucket

        return CompressedMinerIndex(
            sources={
                source: list(labels_to_buckets.values())
                for source, labels_to_buckets in buckets_by_source_by_label.items()
            }
        )

    def clear_content_from_oldest(self, content_bytes_to_clear: int):
        """Deletes entries starting from the oldest until we have cleared the specified amount of content."""
        bt.logging.debug(f"Database full. Clearing {content_bytes_to_clear} bytes.")

        pipeline = [
            {"$sort": {"datetime": 1}},
            {"$limit": content_bytes_to_clear},
            {"$project": {"_id": 1}},
        ]

        result = list(self.db.DataEntity.aggregate(pipeline))
        entry_ids_to_delete = [entry["_id"] for entry in result]

        self.db.DataEntity.delete_many({"_id": {"$in": entry_ids_to_delete}})

    def list_data_entity_buckets(self) -> List[DataEntityBucket]:
        """Lists all DataEntityBuckets for all DataEntities."""
        oldest_time_bucket_id = TimeBucket.from_datetime(
            dt.datetime.now()
            - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
        ).id

        pipeline = [
            {"$match": {"timeBucketId": {"$gte": oldest_time_bucket_id}}},
            {"$group": {"_id": {"timeBucketId": "$timeBucketId", "source": "$source", "label": "$label"}, "bucketSize": {"$sum": "$contentSizeBytes"}}},
            {"$sort": {"bucketSize": -1}},
            {"$limit": constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX},
        ]

        result = list(self.db.DataEntity.aggregate(pipeline))

        data_entity_buckets = []

        for entry in result:
            size = min(entry["bucketSize"], constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)

            data_entity_bucket_id = DataEntityBucketId(
                time_bucket=TimeBucket(id=entry["_id"]["timeBucketId"]),
                source=DataSource(entry["_id"]["source"]),
                label=DataLabel(value=entry["_id"]["label"]) if entry["_id"]["label"] else None,
            )

            data_entity_bucket = DataEntityBucket(
                id=data_entity_bucket_id, size_bytes=size
            )

            data_entity_buckets.append(data_entity_bucket)

        return data_entity_buckets


    def check_labels(self, miner_id):
        self.miner_labels_db = self.meta_db['MinerLabels']
        miner_labels = list(self.miner_labels_db.find({'miner_id': miner_id}))
        if not miner_labels:
            print("In if to find label with id='None")
            miner_labels = list(self.miner_labels_db.find({'miner_id': None}))
            
        print("Miner lables: ",miner_labels)
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
