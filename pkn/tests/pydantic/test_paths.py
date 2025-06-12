from pydantic import BaseModel

from pkn.pydantic import CallablePath, ImportPath


def foo():
    return "foo"


class MyType: ...


class MyModel(BaseModel):
    typ: ImportPath
    foo: CallablePath


def test_get_import_path_inst():
    m = MyModel(typ=MyType, foo=foo)
    assert m.typ == MyType
    assert m.foo == foo


def test_get_import_path_string():
    m = MyModel(typ="pkn.tests.pydantic.test_paths.MyType", foo="pkn.tests.pydantic.test_paths.foo")
    assert m.typ == MyType
    assert m.foo == foo


def test_serialize():
    m = MyModel(typ="pkn.tests.pydantic.test_paths.MyType", foo="pkn.tests.pydantic.test_paths.foo")
    assert m.model_dump() == {
        "typ": MyType,
        "foo": foo,
    }
    assert m.model_dump_json() == ('{"typ":"pkn.tests.pydantic.test_paths.MyType","foo":"pkn.tests.pydantic.test_paths.foo"}')
