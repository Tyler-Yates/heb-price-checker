from dataclasses import dataclass


@dataclass
class ProductDocument:
    id: int
    display_name: str
    category: int
