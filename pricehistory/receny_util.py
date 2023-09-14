import os
import pickle
from datetime import datetime
from typing import Dict, Tuple, Optional

from atomicwrites import atomic_write

from pricehistory.constants import RECENCY_FILE_NAME, RECENCY_MINIMUM_AGE_DAYS


class RecencyUtil:
    def __init__(self, recency_file_path=RECENCY_FILE_NAME):
        self.recency_dict = dict()
        self.recency_file_path = recency_file_path

        if os.path.exists(self.recency_file_path):
            with open(self.recency_file_path, mode="rb") as recency_file:
                self.recency_dict: Dict[str, Tuple] = pickle.load(recency_file)

    def _save_pickle(self):
        with atomic_write(self.recency_file_path, mode="wb", overwrite=True) as recency_file:
            recency_file.write(pickle.dumps(self.recency_dict, protocol=pickle.HIGHEST_PROTOCOL))

    def record_category_page_success(self, category_id: int, after: str):
        self.recency_dict[str(category_id)] = (datetime.now(), after)
        self._save_pickle()

    def get_category_after_cursor(self, category_id: int) -> Optional[str]:
        result = self.recency_dict.get(str(category_id))
        if result:
            return result[1]
        else:
            return None

    def clean_records(self, age_in_days_to_clean=RECENCY_MINIMUM_AGE_DAYS):
        categories_to_remove = []

        for category_id, information_tuple in self.recency_dict.items():
            days_since_record = (datetime.now() - information_tuple[0]).days
            if days_since_record >= age_in_days_to_clean:
                categories_to_remove.append(category_id)

        for category_id in categories_to_remove:
            self.recency_dict.pop(category_id)

        self._save_pickle()
