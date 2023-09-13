import random
from time import sleep
from typing import Optional, List

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from pricechecker.constants import PRODUCTS_QUERY, MAX_SLEEP_SECONDS, MIN_SLEEP_SECONDS


class SourceClient:
    def __init__(self, api_url: str, store_id: str, categories: List[str], cookies: dict):
        self.api_url = api_url
        self.store_id = store_id
        self.categories = categories

        transport = AIOHTTPTransport(url=self.api_url, cookies=cookies)
        self.client = Client(transport=transport)

    def _fetch_category_page(self, category_id: str, after: str = None) -> Optional[str]:
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
            after = f"\"{after}\""

        query = gql(PRODUCTS_QUERY % (category_id, self.store_id, after))
        result = self.client.execute(query)

        print(result)

        # TODO save data to database

        if result["browseCategory"]["hasMoreRecords"]:
            return result["browseCategory"]["nextCursor"]
        else:
            return None

    def process_category(self, category_id: str):
        after_cursor = self._fetch_category_page(category_id)

        while after_cursor is not None:
            # Wait between categories to prevent API throttling
            self._wait_random_time()

            after_cursor = self._fetch_category_page(category_id, after=after_cursor)

    def process_all_categories(self):
        for category in self.categories:
            self.process_category(category)

            # Wait between categories to prevent API throttling
            self._wait_random_time()

    @staticmethod
    def _wait_random_time():
        seconds_to_sleep = random.randint(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS)
        print(f"Sleeping for {seconds_to_sleep} second(s)")
        sleep(seconds_to_sleep)
