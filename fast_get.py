from enum import Enum
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class Category(Enum):
    TOOLS = 'tools'
    CONSUMABLES = 'consumables'


class Item(BaseModel):
    name: str
    price: float
    count: int
    id: int
    category: Category


items = {
    0: Item(name="Hammer", price=12.45, count=20, id=131, category=Category.TOOLS),
    1: Item(name="ScrewDriver", price=3.5, count=35, id=94, category=Category.TOOLS),
    2: Item(name="Nails", price=2.99, count=15, id=45, category=Category.CONSUMABLES),
}


@app.get("/")
def index() -> dict[str, dict[int, Item]]:
    return {'items': items}


@app.get("/items/{item_id}")
def query_item_by_id(item_id: int) -> Item:
    if item_id not in Item:
        return items[1]
    return items[0]
