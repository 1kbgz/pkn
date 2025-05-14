from typing import Dict as DictType, List as ListType, Optional, Union

from pydantic import BaseModel, Field, RootModel

__all__ = ("Dict", "List")


class Dict(RootModel):
    root: Optional[DictType[str, Optional[Union[BaseModel, "Dict", "List", int, float, str]]]] = Field(default_factory=dict)

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item: str):
        return self.root[item]

    def __setitem__(self, key: str, value: BaseModel):
        self.root[key] = value

    def __delitem__(self, key: str):
        del self.root[key]

    def __len__(self):
        return len(self.root)

    def __contains__(self, item: str):
        return item in self.root

    def keys(self):
        return self.root.keys()

    def values(self):
        return self.root.values()

    def items(self):
        return self.root.items()

    def get(self, key: str, default=None):
        return self.root.get(key, default)

    def pop(self, key: str, default=None):
        return self.root.pop(key, default)

    def clear(self):
        self.root.clear()


class List(RootModel):
    root: Optional[ListType[Union[BaseModel, "Dict", "List", int, float, str]]] = Field(default_factory=list)

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item: int):
        return self.root[item]

    def __setitem__(self, key: int, value: BaseModel):
        self.root[key] = value

    def __delitem__(self, key: int):
        del self.root[key]

    def __len__(self):
        return len(self.root)

    def __contains__(self, item: BaseModel):
        return item in self.root

    def append(self, value: BaseModel):
        self.root.append(value)

    def extend(self, values: ListType[BaseModel]):
        self.root.extend(values)

    def insert(self, index: int, value: BaseModel):
        self.root.insert(index, value)

    def remove(self, value: BaseModel):
        self.root.remove(value)

    def pop(self, index: int):
        return self.root.pop(index)

    def clear(self):
        self.root.clear()

    def index(self, value: BaseModel):
        return self.root.index(value)

    def count(self, value: BaseModel):
        return self.root.count(value)

    def reverse(self):
        self.root.reverse()

    def sort(self, key=None, reverse=False):
        self.root.sort(key=key, reverse=reverse)

    def copy(self):
        return self.root.copy()

    def __add__(self, other):
        return self.root + other

    def __iadd__(self, other):
        self.root += other
        return self
