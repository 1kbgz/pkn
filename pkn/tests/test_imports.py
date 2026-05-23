def test_imports():
    import pkn

    # Test lazy-loaded submodules
    infra = pkn.infra
    pydantic = pkn.pydantic

    assert infra is not None
    assert pydantic is not None

    from pkn import infra, pydantic

    assert infra is not None
    assert pydantic is not None

    # Test lazy-loaded classes from pydantic
    Dict = pkn.Dict
    List = pkn.List

    assert Dict is not None
    assert List is not None

    from pkn.pydantic import Dict, List

    assert Dict is not None
    assert List is not None

    # Test lazy-loaded functions from logging
    default = pkn.default
    getLogger = pkn.getLogger
    getSimpleLogger = pkn.getSimpleLogger

    assert default is not None
    assert getLogger is not None
    assert getSimpleLogger is not None

    from pkn import default, getLogger, getSimpleLogger

    assert default is not None
    assert getLogger is not None
    assert getSimpleLogger is not None
    from pkn.logging import default, getLogger, getSimpleLogger

    assert default is not None
    assert getLogger is not None
    assert getSimpleLogger is not None
