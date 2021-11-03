from arango_orm import Collection
from arango_orm.fields import String, Date

class Sneakers(Collection):
    __collection__ = "Sneakers"
    _index = []