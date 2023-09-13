import dataclasses

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.server_api import ServerApi

from .data.category_document import CategoryDocument
from .data.price_document import PriceDocument
from .data.product_document import ProductDocument


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
        self.categories_collection.create_index([("id", pymongo.ASCENDING)], unique=True)
        self.categories_collection.create_index([("product_id", pymongo.ASCENDING)], unique=False)

    def save_product_price(
        self, price_document: PriceDocument, product_display_name: str, category_id: int, category_display_name: str
    ):
        self._ensure_category_exists(category_id, category_display_name)
        self._ensure_product_exists(price_document, product_display_name, category_id)

        self.prices_collection.update_one(
            filter={"product_id": price_document.product_id, "start_date": price_document.start_date},
            update={"$set": dataclasses.asdict(price_document)},
            upsert=True,
        )

        print(f"Upserted price document {price_document}")

    def _ensure_product_exists(self, price_document: PriceDocument, product_display_name: str, category_id: int):
        product_document = ProductDocument(
            id=price_document.product_id, display_name=product_display_name, category=category_id
        )

        self.products_collection.update_one(
            filter={"id": price_document.product_id}, update={"$set": dataclasses.asdict(product_document)}, upsert=True
        )

    def _ensure_category_exists(self, category_id: int, category_display_name: str):
        category_document = CategoryDocument(id=category_id, display_name=category_display_name)

        self.categories_collection.update_one(
            filter={"id": category_document.id}, update={"$set": dataclasses.asdict(category_document)}, upsert=True
        )
