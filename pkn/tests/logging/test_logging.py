from io import StringIO
from unittest.mock import patch


class TestLogger:
    def test_logging(self):
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
