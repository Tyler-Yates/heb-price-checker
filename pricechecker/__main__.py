import json

from pricechecker.source_client import SourceClient


def main():
    with open("config.json") as config_file:
        config_json = json.load(config_file)
        api_url = config_json["apiUrl"]
        categories = config_json["categories"]
        store_id = config_json["storeId"]
        cookies = config_json["cookies"]

    source_client = SourceClient(api_url=api_url, store_id=store_id, categories=categories, cookies=cookies)
    source_client.process_all_categories()


if __name__ == '__main__':
    main()
