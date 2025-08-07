from unittest.mock import patch

from pkn import default, getLogger


def test():
    default()
    logger = getLogger()
    logger.critical("This is a test log message.")


class TestLogger:
    def test_logging(self):
        # Patch standard out and look at log message
        with patch("sys.stdout") as mock_stdout:
            default()
            logger = getLogger()
            logger.critical("This is a test log message.")
            assert mock_stdout.write.call_args[0][0].endswith(
                "\x1b[0m][MainThread][\x1b[34mpkn.tests.logging.test_logging\x1b[0m][\x1b[31mCRITICAL\x1b[0m]: This is a test log message.\x1b[0m\n"
            )
