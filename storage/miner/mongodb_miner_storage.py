from common import constants, utils
from common.data import (
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)
from storage.miner.miner_storage import MinerStorage
from typing import List
import datetime as dt
import pymongo
import bittensor as bt


class MongodbMinerStorage(MinerStorage):
    """MongoDB backed MinerStorage"""

    def __init__(
        self,
        mongodb_uri="mongodb://localhost:27017/",
        mongodb_database="mongodb_miner_storage",
        max_database_size_gb_hint=250,
    ):
        # TODO Account for non-content columns when restricting total database size.
        self.database_max_content_size_bytes = utils.gb_to_bytes(
            max_database_size_gb_hint
        )
        try:
            self.mongodb_uri = mongodb_uri
            self.mongodb_database = mongodb_database
            print("MOngo: ", self.mongodb_uri, self.mongodb_database)

            self.mongodb_client = pymongo.MongoClient(self.mongodb_uri)
            self.mongodb_db = self.mongodb_client[self.mongodb_database]

        except Exception as e:
            print("Error in mongodb creation: ", e)

    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""

        added_content_size = sum(
            data_entity.content_size_bytes for data_entity in data_entities
        )

        # If the total size of the store is larger than our maximum configured stored content size, then raise an exception.
        if added_content_size > self.database_max_content_size_bytes:
            raise ValueError(
                f"Content size to store: {added_content_size} exceeds configured max: {self.database_max_content_size_bytes}"
            )

        data_entity_collection = self.mongodb_db["DataEntity"]

        for data_entity in data_entities:
            label = "NULL" if data_entity.label is None else data_entity.label.value
            time_bucket_id = TimeBucket.from_datetime(data_entity.datetime).id

            data_entity_document = {
                "uri": data_entity.uri,
                "datetime": data_entity.datetime,
                "timeBucketId": time_bucket_id,
                "source": data_entity.source,
                "label": label,
                "content": data_entity.content,
                "contentSizeBytes": data_entity.content_size_bytes,
            }

            # Use update_one with upsert=True to perform an upsert operation (replace if exists, insert if not).
            data_entity_collection.update_one(
                {"uri": data_entity.uri},
                {"$set": data_entity_document},
                upsert=True,
            )


    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucketId."""
        label = (
            "NULL"
            if (data_entity_bucket_id.label is None)
            else data_entity_bucket_id.label.value
        )
        # MongoDB-specific logic for retrieving data entities
        # Implement based on your MongoDB schema and requirements

        data_entity_collection = self.mongodb_db["DataEntity"]

        cursor = data_entity_collection.find(
            {
                "timeBucketId": data_entity_bucket_id.time_bucket.id,
                "source": data_entity_bucket_id.source,
                "label": label,
            }
        )

        # Convert the cursor into DataEntity objects and return them up to the configured max chunk size.
        data_entities = []
        running_size = 0

        for document in cursor:
            if (
                running_size + document["contentSizeBytes"]
                >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
            ):
                # If we would go over the max DataEntityBucket size, instead return early.
                return data_entities
            else:
                # Construct the new DataEntity with all non-null columns.
                data_entity = DataEntity(
                    uri=document["uri"],
                    datetime=document["datetime"],
                    source=DataSource(document["source"]),
                    content=document["content"],
                    content_size_bytes=document["contentSizeBytes"],
                )

                # Add the optional Label field if not null.
                if document["label"] != "NULL":
                    data_entity.label = DataLabel(value=document["label"])

                data_entities.append(data_entity)
                running_size += document["contentSizeBytes"]

        # If we reach the end of the cursor, then return all of the data entities for this DataEntityBucket.
        bt.logging.trace(
            f"Returning {len(data_entities)} data entities for bucket {data_entity_bucket_id}"
        )
        return data_entities

    def list_data_entity_buckets(self) -> List[DataEntityBucket]:
        """Lists all DataEntityBuckets for all the DataEntities that this MinerStorage is currently serving."""

        data_entity_collection = self.mongodb_db["DataEntity"]

        oldest_time_bucket_id = TimeBucket.from_datetime(
            dt.datetime.now()
            - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
        ).id

        # Use the aggregation pipeline to group by timeBucketId, source, and label and calculate the sum of contentSizeBytes.
        pipeline = [
            {
                "$match": {"datetime": {"$gte": oldest_time_bucket_id}},
            },
            {
                "$group": {
                    "_id": {
                        "timeBucketId": "$timeBucketId",
                        "source": "$source",
                        "label": "$label",
                    },
                    "bucketSize": {"$sum": "$contentSizeBytes"},
                },
            },
            {
                "$sort": {"bucketSize": -1},
            },
            {
                "$limit": constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
            },
        ]

        cursor = data_entity_collection.aggregate(pipeline)

        data_entity_buckets = []

        for document in cursor:
            # Ensure the miner does not attempt to report more than the max DataEntityBucket size.
            size = (
                constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                if document["bucketSize"]
                >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                else document["bucketSize"]
            )

            # Construct the new DataEntityBucket with all non-null columns.
            data_entity_bucket_id = DataEntityBucketId(
                time_bucket=TimeBucket(id=document["_id"]["timeBucketId"]),
                source=DataSource(document["_id"]["source"]),
            )

            # Add the optional Label field if not None.
            if document["_id"]["label"] is not None:
                data_entity_bucket_id.label = DataLabel(value=document["_id"]["label"])

            data_entity_bucket = DataEntityBucket(
                id=data_entity_bucket_id, size_bytes=size
            )

            data_entity_buckets.append(data_entity_bucket)

        # If we reach the end of the cursor, then return all of the data entity buckets.
        return data_entity_buckets
