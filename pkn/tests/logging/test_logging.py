from io import StringIO
from unittest.mock import patch


class TestLogger:
    def test_logging(self, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.setenv("FORCE_COLOR", "1")
        # Patch standard out and look at log message
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            from pkn import default, getLogger

            default()
            logger = getLogger()
            logger.critical("This is a test log message 2.")
            assert mock_stdout.getvalue().endswith(
                "\x1b[0m][MainThread][\x1b[34mpkn.tests.logging.test_logging\x1b[0m][\x1b[31mCRITICAL\x1b[0m]: This is a test log message 2.\x1b[0m\n"
            )

    def test_simple_logger(self):
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            from pkn import getSimpleLogger

            logger = getSimpleLogger("test_simple_logger", stdout=True)
            logger.info("This is a simple log message.")
            assert mock_stdout.getvalue().endswith("][test_simple_logger][INFO]: This is a simple log message.\n")

    def test_logger_without_frame(self):
        from logging import getLogger as base_get_logger

        from pkn import getLogger

        with patch("pkn.logging.currentframe", return_value=None):
            assert getLogger() is base_get_logger()

    def test_logger_without_loader(self):
        from types import SimpleNamespace

        from pkn import getLogger

        frame = SimpleNamespace(f_back=SimpleNamespace(f_globals={"__name__": "dynamic_module"}))
        with patch("pkn.logging.currentframe", return_value=frame):
            assert getLogger().name == "dynamic_module"
