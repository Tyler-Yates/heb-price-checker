from dataclasses import dataclass
from datetime import datetime


@dataclass
class PriceDocument:
    product_id: int
    price_cents: int
    start_date: datetime
