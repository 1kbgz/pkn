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
