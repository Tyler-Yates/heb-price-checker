import datetime
import random
from time import sleep
from typing import Optional, List

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from .data.category_document import CategoryDocument
from .data.price_container import PriceContainer
from .data.price_document import PriceDocument
from pricehistory.constants import PRODUCTS_QUERY, MAX_SLEEP_SECONDS, MIN_SLEEP_SECONDS, RECENCY_CATEGORY_COMPLETE
from pricehistory.db_client import DBClient
from .data.product_document import ProductDocument
from .receny_util import RecencyUtil


class SourceClient:
    def __init__(
        self,
        api_url: str,
        store_id: str,
        categories: List[int],
        cookies: dict,
        db_client: DBClient,
        recency_util: RecencyUtil,
    ):
        self.api_url = api_url
        self.store_id = store_id
        self.categories = categories
        self.db_client = db_client
        self.recency_util = recency_util

        self.today = datetime.datetime.today()

        transport = AIOHTTPTransport(url=self.api_url, cookies=cookies)
        self.client = Client(transport=transport)

    @staticmethod
    def _parse_price_string(price_string: str) -> int:
        if " for " in price_string:
            parts = price_string.split(" for ")
            quantity = parts[0]
            total = parts[1]

            return int(float(total.replace("$", "")) / int(quantity) * 100)
        else:
            return int(float(price_string.replace("$", "")) * 100)

    def _get_price_cents(self, record: dict) -> Optional[int]:
        for sku in record["SKUs"]:
            for context in sku["contextPrices"]:
                if context["context"].lower() != "online":
                    continue

                sale_price = context["salePrice"]
                if sale_price:
                    return self._parse_price_string(sale_price["formattedAmount"])
                else:
                    return self._parse_price_string(context["listPrice"]["formattedAmount"])

        return None

    @staticmethod
    def _get_product_size(record: dict) -> str:
        for sku in record["SKUs"]:
            if "customerFriendlySize" in sku:
                return sku["customerFriendlySize"]

        return ""

    def _process_records(self, records: dict, category_id: int, category_display_name: str):
        price_containers = []
        for record in records:
            price_cents = self._get_price_cents(record)
            product_id = int(record["id"])
            price_document = PriceDocument(product_id=product_id, price_cents=price_cents, start_date=self.today)

            product_display_name = record["displayName"]

            # Some products share the same display name and only differ by size
            product_size = self._get_product_size(record)
            if product_size and product_size.lower() != "each":
                product_display_name += f" ({product_size})"

            product_document = ProductDocument(id=product_id, display_name=product_display_name, category=category_id)

            price_container = PriceContainer(product_document=product_document, price_document=price_document)
            price_containers.append(price_container)

        category_document = CategoryDocument(id=category_id, display_name=category_display_name)
        self.db_client.save_product_prices(price_containers=price_containers, category_document=category_document)

    def _fetch_category_page(self, category_id: int, after: str = None) -> Optional[str]:
        """
        Fetches a single page for a given category.

        Args:
            category_id: The ID of the category
            after: The cursor to use for pagination

        Returns:
            The 'after' cursor if there is at least one more page to process
        """
        if after is None:
            after = "null"
        else:
            after = f'"{after}"'

        query = gql(PRODUCTS_QUERY % (category_id, self.store_id, after))
        result = self.client.execute(query)

        print(result)
        category_display_name = result["browseCategory"]["pageTitle"]
        self._process_records(result["browseCategory"]["records"], category_id, category_display_name)

        if result["browseCategory"]["hasMoreRecords"]:
            # Save the cursor so that if the program crashes we can skip pages we already have done
            next_cursor = result["browseCategory"]["nextCursor"]
            self.recency_util.record_category_page_success(category_id, next_cursor)
            return next_cursor
        else:
            # No more pages, so log that the category is complete
            self.recency_util.record_category_page_success(category_id, RECENCY_CATEGORY_COMPLETE)
            return None

    def _fetch_category_page_with_retry(self, category_id: int, after: str = None) -> Optional[str]:
        for i in range(5):
            try:
                return self._fetch_category_page(category_id, after)
            except Exception as e:
                print(f"Exception fetching category page: {e}")

        raise ValueError("Failed to fetch page")

    def process_category(self, category_id: int):
        # See if we have already processed some pages in this category recently
        after_cursor = self.recency_util.get_category_after_cursor(category_id)
        if after_cursor == RECENCY_CATEGORY_COMPLETE:
            print(f"Skipping category {category_id} as it is already complete")
            return
        else:
            print(f"Starting with cursor {after_cursor} for category {category_id}")

        # None means we have not completed the first page so grab that first
        if after_cursor is None:
            after_cursor = self._fetch_category_page_with_retry(category_id)

        # Keep grabbing pages until we're done
        while after_cursor is not None:
            after_cursor = self._fetch_category_page_with_retry(category_id, after=after_cursor)

    def process_all_categories(self):
        for category in self.categories:
            self.process_category(category)

    @staticmethod
    def _wait_random_time():
        seconds_to_sleep = random.randint(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS)
        print(f"Sleeping for {seconds_to_sleep} second(s)")
        sleep(seconds_to_sleep)
