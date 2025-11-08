def test_imports():
    import pkn

    # Test lazy-loaded submodules
    ccflow = pkn.ccflow
    infra = pkn.infra
    pydantic = pkn.pydantic

    assert ccflow is not None
    assert infra is not None
    assert pydantic is not None

    from pkn import ccflow, infra, pydantic

    assert ccflow is not None
    assert infra is not None
    assert pydantic is not None

    # Test lazy-loaded classes from pydantic
    Message = pkn.Message
    SMTP = pkn.SMTP
    Attachment = pkn.Attachment
    Email = pkn.Email
    Dict = pkn.Dict
    List = pkn.List

    assert Message is not None
    assert SMTP is not None
    assert Attachment is not None
    assert Email is not None
    assert Dict is not None
    assert List is not None

    from pkn import SMTP, Attachment, Dict, Email, List, Message

    assert Message is not None
    assert SMTP is not None
    assert Attachment is not None
    assert Email is not None
    assert Dict is not None
    assert List is not None

    from pkn.pydantic import SMTP, Attachment, Dict, Email, List, Message

    assert Message is not None
    assert SMTP is not None
    assert Attachment is not None
    assert Email is not None
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
