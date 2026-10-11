from typing import Any, Union, cast

from pydantic import BaseModel, Field, RootModel

__all__ = ("Dict", "List")


class Dict(RootModel):
    root: dict[str, Union[BaseModel, "Dict", "List", int, float, str] | None] | None = Field(default_factory=dict)

    @property
    def _container(self):
        if self.root is None:
            raise TypeError("root is None")
        return self.root

    def __iter__(self):
        return iter(self._container)

    def __getitem__(self, item: str):
        return self._container[item]

    def __setitem__(self, key: str, value: BaseModel):
        self._container[key] = value

    def __delitem__(self, key: str):
        del self._container[key]

    def __len__(self):
        return len(self._container)

    def __contains__(self, item: str):
        return item in self._container

    def keys(self):
        return self._container.keys()

    def values(self):
        return self._container.values()

    def items(self):
        return self._container.items()

    def get(self, key: str, default=None):
        return self._container.get(key, default)

    def pop(self, key: str, default=None):
        return self._container.pop(key, default)

    def clear(self):
        self._container.clear()


class List(RootModel):
    root: list[Union[BaseModel, "Dict", "List", int, float, str] | None] | None = Field(default_factory=list)

    @property
    def _container(self):
        if self.root is None:
            raise TypeError("root is None")
        return self.root

    def __iter__(self):
        return iter(self._container)

    def __getitem__(self, item: int):
        return self._container[item]

    def __setitem__(self, key: int, value: BaseModel):
        self._container[key] = value

    def __delitem__(self, key: int):
        del self._container[key]

    def __len__(self):
        return len(self._container)

    def __contains__(self, item: BaseModel):
        return item in self._container

    def append(self, value: BaseModel):
        self._container.append(value)

    def extend(self, values: list[BaseModel]):
        self._container.extend(values)

    def insert(self, index: int, value: BaseModel):
        self._container.insert(index, value)

    def remove(self, value: BaseModel):
        self._container.remove(value)

    def pop(self, index: int):
        return self._container.pop(index)

    def clear(self):
        self._container.clear()

    def index(self, value: BaseModel):
        return self._container.index(value)

    def count(self, value: BaseModel):
        return self._container.count(value)

    def reverse(self):
        self._container.reverse()

    def sort(self, key=None, reverse=False):
        cast(list[Any], self._container).sort(key=key, reverse=reverse)

    def copy(self):  # ty: ignore[invalid-method-override]  # Public API returns a list, not a model copy.
        return self._container.copy()

    def __add__(self, other):
        return self._container + other

    def __iadd__(self, other):
        self._container.extend(other)
        return self
