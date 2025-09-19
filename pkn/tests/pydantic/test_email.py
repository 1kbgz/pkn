from os import environ

import pytest

from pkn.pydantic.email import SMTP, Attachment, Email, Message


class TestEmail:
    def test_email_creation(self):
        email = Email(
            message=Message(html="<p>Hello, World!</p>", text="Hello, World!", subject="Test Email", mail_from=("Test", "test@example.com")),
            smtp=SMTP(host="smtp.example.com", port=587, user="user", password="pass", tls=True),
            attachments=[Attachment(filename="test.txt", content_disposition="attachment", data=b"Test file content")],
        )
        assert email.message.subject == "Test Email"
        assert email.smtp.host == "smtp.example.com"
        assert email.attachments[0].filename == "test.txt"

    @pytest.mark.skipif(not environ.get("SMTP_HOST"), reason="SMTP server not configured")
    def test_email_send(self):
        email = Email(
            message=Message(
                html="<p>Hello, World!</p>",
                text="Hello, World!",
                subject="Test Email",
                mail_from=("Test", environ.get("SMTP_USER", "test@example.com")),
            ),
            smtp=SMTP(
                host=environ.get("SMTP_HOST", "smtp.example.com"),
                port=environ.get("SMTP_PORT", 587),
                user=environ.get("SMTP_USER", "user"),
                password=environ.get("SMTP_PASS", "pass"),
                tls=True,
            ),
            attachments=[Attachment(filename="test.txt", content_disposition="attachment", data=b"Test file content")],
        )
        response = email.send(to=environ.get("SMTP_USER", "recipient@example.com"))
        assert response.status_code == 250
