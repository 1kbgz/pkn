import pytest
from pydantic import BaseModel

from pkn.pydantic import Dict, List


class MyModel(BaseModel):
    a: int
    b: str


class TestPydanticRoots:
    def test_dict(self):
        d = Dict({"a": MyModel(a=1, b="b"), "b": MyModel(a=2, b="c")})
        assert len(d) == 2
        assert "a" in d
        assert "b" in d
        assert d["a"] == MyModel(a=1, b="b")
        assert d["b"] == MyModel(a=2, b="c")
        d["a"] = MyModel(a=3, b="d")
        assert d["a"] == MyModel(a=3, b="d")
        del d["a"]
        assert len(d) == 1
        assert "a" not in d
        assert "b" in d
        d.clear()
        assert len(d) == 0
        assert "a" not in d
        assert "b" not in d

    def test_dict_types(self):
        d = Dict({"a": MyModel(a=1, b="b"), "b": "abc", "c": 1, "d": 1.2, "e": List(), "f": Dict(), "g": None})
        assert len(d) == 7

    def test_list(self):
        lst = List([MyModel(a=1, b="b"), MyModel(a=2, b="c")])
        assert len(lst) == 2
        assert lst[0] == MyModel(a=1, b="b")
        assert lst[1] == MyModel(a=2, b="c")
        lst[0] = MyModel(a=3, b="d")
        assert lst[0] == MyModel(a=3, b="d")
        del lst[0]
        assert len(lst) == 1
        assert lst[0] == MyModel(a=2, b="c")

    def test_list_types(self):
        d = List([MyModel(a=1, b="b"), "abc", 1, 1.2, List(), Dict(), None])
        assert len(d) == 7

    @pytest.mark.parametrize("model", [Dict, List])
    def test_null_root(self, model):
        value = model(None)
        assert value.root is None
        with pytest.raises(TypeError, match="root is None"):
            len(value)

    def test_list_sort_copy_and_iadd(self):
        value = List([3, 1, 2])
        value.sort()
        assert value.copy() == [1, 2, 3]
        root = value.root
        value += [4]
        assert value.root is root
        assert value.copy() == [1, 2, 3, 4]
        value.sort(key=lambda item: -item)
        assert value.copy() == [4, 3, 2, 1]
