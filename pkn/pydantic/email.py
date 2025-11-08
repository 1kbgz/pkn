from typing import Union

from ccflow import BaseModel
from pydantic import Field

__all__ = (
    "Message",
    "SMTP",
    "Attachment",
    "Email",
)


class Message(BaseModel):
    html: str = Field(default=None, description="HTML content of the email")
    text: str = Field(default=None, description="Plain text content of the email")
    subject: str = Field(default=None, description="Subject of the email")
    mail_from: Union[tuple[str, str], str] = Field(default=None, description="Sender email address")


class SMTP(BaseModel):
    host: str = Field(..., description="SMTP server host")
    port: int = Field(default=25, description="SMTP server port")
    user: str = Field(default=None, description="SMTP server username")
    password: str = Field(default=None, description="SMTP server password")
    tls: bool = Field(default=False, description="Use TLS for SMTP connection")
    ssl: bool = Field(default=False, description="Use SSL for SMTP connection")
    timeout: int = Field(default=5, description="Timeout for SMTP connection in seconds")


class Attachment(BaseModel):
    filename: str = Field(..., description="Name of the attachment file")
    content_disposition: str = Field(default="attachment", description="Content disposition of the attachment")
    data: bytes = Field(..., description="Binary data of the attachment")


class Email(BaseModel):
    message: Message = Field(default_factory=Message, description="Email message details")
    smtp: SMTP = Field(default_factory=SMTP, description="SMTP server configuration")
    attachments: list[Attachment] = Field(default_factory=list, description="List of email attachments")

    def send(self, to: Union[str, list[str]], render: dict = None):
        # NOTE: defer import
        from emails import Message as EmailMessage

        msg = EmailMessage(html=self.message.html, text=self.message.text, subject=self.message.subject, mail_from=self.message.mail_from)

        for attachment in self.attachments:
            msg.attach(filename=attachment.filename, content_disposition=attachment.content_disposition, data=attachment.data)

        smtp_config = self.smtp.model_dump(exclude_none=True, exclude=["type_"])
        response = msg.send(to=to, render=render or {}, smtp=smtp_config)
        return response
