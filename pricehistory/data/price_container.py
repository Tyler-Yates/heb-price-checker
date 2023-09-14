from dataclasses import dataclass

from pricehistory.data.price_document import PriceDocument
from pricehistory.data.product_document import ProductDocument


@dataclass
class PriceContainer:
    product_document: ProductDocument
    price_document: PriceDocument
