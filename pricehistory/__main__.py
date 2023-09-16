import json
import logging

import redis
import requests

from pricehistory.constants import DB_CONNECTION_STRING
from pricehistory.cookie_util import get_cookies
from pricehistory.db_client import DBClient
from pricehistory.receny_util import RecencyUtil
from pricehistory.source_client import SourceClient


logging.basicConfig(level=logging.INFO)
logging.getLogger("engineio.server").setLevel(logging.WARNING)
logging.getLogger("socketio.server").setLevel(logging.WARNING)

LOG = logging.getLogger(__name__)


def main():
    with open("config.json") as config_file:
        config_json = json.load(config_file)
        api_url = config_json["apiUrl"]
        categories = config_json["categories"]
        cookie_url = config_json["cookieUrl"]
        store_id = config_json["storeId"]
        db_username = config_json["db_username"]
        db_password = config_json["db_password"]
        db_host = config_json["db_host"]
        healthcheck_url = config_json["healthcheck_url"]
        data_cache_url = config_json["data_cache_url"]

    if data_cache_url:
        cache = redis.Redis.from_url(data_cache_url)
        cache.ping()
        LOG.info("Using Redis cache for data")
    else:
        cache = None

    db_connection_string = DB_CONNECTION_STRING % (db_username, db_password, db_host)
    db_client = DBClient(db_connection_string=db_connection_string, cache=cache)

    cookies = get_cookies(cookie_url)

    recency_util = RecencyUtil()
    recency_util.clean_records()

    source_client = SourceClient(
        api_url=api_url,
        store_id=store_id,
        categories=categories,
        cookies=cookies,
        db_client=db_client,
        recency_util=recency_util,
    )
    source_client.process_all_categories()

    print("Pinging healthcheck URL...")
    response = requests.get(healthcheck_url)
    response.raise_for_status()
    print("Done!")


if __name__ == "__main__":
    main()
