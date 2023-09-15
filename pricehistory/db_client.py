import dataclasses
from typing import List

import pymongo
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.operations import SearchIndexModel
from pymongo.server_api import ServerApi

from .data.category_document import CategoryDocument
from .data.price_container import PriceContainer


class DBClient:
    def __init__(self, db_connection_string: str):
        self.client = MongoClient(db_connection_string, server_api=ServerApi("1"))

        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command("ping")
            print("Successfully connected to database!")
        except Exception as e:
            print(e)

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
        self._ensure_products_exist(price_containers)

        # Save all the prices to the database
        self._ensure_prices_exist(price_containers)

    def _ensure_products_exist(self, price_containers: List[PriceContainer]):
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
            print("Upserted 0 product documents")
            return

        result = self.products_collection.bulk_write(operations)
        print(f"Upserted/Modified {result.upserted_count + result.modified_count} product documents")

    def _ensure_prices_exist(self, price_containers: List[PriceContainer]):
        # Determine which price documents actually need to be saved
        operations = []
        for price_container in price_containers:
            price_document = price_container.price_document

            most_recent_price_document = self.prices_collection.find_one(
                filter={"product_id": price_document.product_id}, sort=[("start_date", pymongo.DESCENDING)]
            )

            if most_recent_price_document:
                most_recent_price = most_recent_price_document["price_cents"]
                if most_recent_price == price_document.price_cents:
                    print(f"Skipping update for product {price_document.product_id} as the price is unchanged")
                    continue

            print(f"Updating price for product {price_document.product_id}")
            operation = UpdateOne(
                filter={"product_id": price_document.product_id, "start_date": price_document.start_date},
                update={"$set": dataclasses.asdict(price_document)},
                upsert=True,
            )
            operations.append(operation)

        if not operations:
            print("Upserted 0 price documents")
            return

        result = self.prices_collection.bulk_write(operations)
        print(f"Upserted {result.upserted_count} price documents")

    def _ensure_category_exists(self, category_document: CategoryDocument):
        self.categories_collection.update_one(
            filter={"id": category_document.id}, update={"$set": dataclasses.asdict(category_document)}, upsert=True
        )
