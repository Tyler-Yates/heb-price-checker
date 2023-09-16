import dataclasses
import logging
from typing import List

import fakeredis
import pymongo
import redis
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.operations import InsertOne
from pymongo.server_api import ServerApi

from .constants import (
    REDIS_VERSION,
    PRODUCT_PRICE_HISTORY_CACHE_PREFIX,
    CATEGORY_PRODUCTS_CACHE_KEY,
    CATEGORY_NAME_CACHE_KEY,
)
from .data.category_document import CategoryDocument
from .data.price_container import PriceContainer


LOG = logging.getLogger(__name__)


class DBClient:
    def __init__(self, db_connection_string: str, cache: redis.Redis = None):
        # If no cache is given, spin up a fake one
        if cache is None:
            self.cache = fakeredis.FakeStrictRedis(version=REDIS_VERSION)
        else:
            self.cache = cache

        self.client = MongoClient(db_connection_string, server_api=ServerApi("1"))

        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command("ping")
            LOG.info("Successfully connected to database!")
        except Exception:
            LOG.exception("Error connecting to database!")

        # Collections
        self.database = self.client["price_history"]
        self.products_collection: Collection = self.database["products"]
        self.categories_collection: Collection = self.database["categories"]
        self.prices_collection: Collection = self.database["prices"]

        # Indexes
        self.products_collection.create_index([("id", pymongo.ASCENDING)], unique=True)
        self.products_collection.create_index([("display_name", pymongo.TEXT)])
        self.categories_collection.create_index([("id", pymongo.ASCENDING)], unique=True)
        self.prices_collection.create_index(
            [("product_id", pymongo.ASCENDING), ("start_date", pymongo.DESCENDING)], unique=False
        )

        # TODO uncomment this when pymongo supports creating a search index
        # Search index for string searching
        # search_index_definition = {
        #     "mappings": {
        #         "dynamic": False,
        #         "fields": {
        #             "display_name": [
        #                 {
        #                     "foldDiacritics": True,
        #                     "maxGrams": 6,
        #                     "minGrams": 3,
        #                     "tokenization": "nGram",
        #                     "type": "autocomplete",
        #                 }
        #             ]
        #         },
        #     }
        # }
        # search_index_model = SearchIndexModel(name="default", definition=search_index_definition)
        # self.products_collection.create_search_index(search_index_model)

    def save_product_prices(self, price_containers: List[PriceContainer], category_document: CategoryDocument):
        # Ensure we have a document for the category
        self._ensure_category_exists(category_document)

        # Ensure we have a document for each product
        self._ensure_products_exist(price_containers, category_document)

        # Save all the prices to the database
        self._ensure_prices_exist(price_containers)

    def _ensure_products_exist(self, price_containers: List[PriceContainer], category_document: CategoryDocument):
        operations = []
        for price_container in price_containers:
            product_document = price_container.product_document
            operations.append(
                UpdateOne(
                    filter={"id": product_document.id},
                    update={"$set": dataclasses.asdict(product_document)},
                    upsert=True,
                )
            )

        if not operations:
            LOG.info("Upserted 0 product documents")
            return

        result = self.products_collection.bulk_write(operations)
        num_documents_changed = result.upserted_count + result.modified_count
        LOG.info(f"Upserted/Modified {num_documents_changed} product documents")

        if num_documents_changed > 0:
            # We should reset this category's cache to show the new product data
            cache_key = f"{CATEGORY_PRODUCTS_CACHE_KEY}_{category_document.id}"
            self.cache.delete(cache_key)
            # We should also reset the product display name cache for any products
            # that actually changed, but this is not easy to do since bulk_write
            # does not return the ids of modified documents, only the ones that
            # were upserted (did not exist before). Instead, we just rely on the
            # one-day cache TTL and know that there may be stale product names
            # for a single day.

    def _ensure_prices_exist(self, price_containers: List[PriceContainer]):
        # Determine which price documents actually need to be saved
        operations = []
        for price_container in price_containers:
            price_document = price_container.price_document
            product_id = price_document.product_id

            # To save space in the database, we only want to insert documents when the price changes
            most_recent_price_document = self.prices_collection.find_one(
                filter={"product_id": product_id}, sort=[("start_date", pymongo.DESCENDING)]
            )
            if most_recent_price_document:
                most_recent_price = most_recent_price_document["price_cents"]
                if most_recent_price == price_document.price_cents:
                    LOG.info(f"Skipping update for product {product_id} as the price is unchanged")
                    continue
                else:
                    LOG.info(f"Updating price for product {product_id}")
            else:
                LOG.info(f"New product found: {product_id}")

            operation = InsertOne(dataclasses.asdict(price_document))
            operations.append(operation)

        if operations:
            result = self.prices_collection.bulk_write(operations)
            LOG.info(f"Inserted {result.inserted_count} price documents")
        else:
            LOG.info("Inserted 0 price documents")

        # We want to wait until the database update happens before we unset the cache entries for any new products or
        # products that changed prices.
        for operation in operations:
            product_id = operation._doc["product_id"]
            cache_key = f"{PRODUCT_PRICE_HISTORY_CACHE_PREFIX}_{product_id}"
            self.cache.delete(cache_key)

    def _ensure_category_exists(self, category_document: CategoryDocument):
        update_result = self.categories_collection.update_one(
            filter={"id": category_document.id}, update={"$set": dataclasses.asdict(category_document)}, upsert=True
        )

        if update_result.modified_count > 0:
            cache_key = f"{CATEGORY_NAME_CACHE_KEY}_{category_document.id}"
            self.cache.delete(cache_key)
