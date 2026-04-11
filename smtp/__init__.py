"""Thread‑safe SMTP helper with typed API and automatic retries.
"""

import os
import smtplib
import threading
import time
import traceback
import json
import re
import urllib.request
import urllib.error
from contextlib import suppress
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SMTP_HOST: str = os.environ["DUO_SMTP_HOST"]
SMTP_PORT: int = int(os.environ["DUO_SMTP_PORT"])
SMTP_USER: str = os.environ["DUO_SMTP_USER"]
SMTP_PASS: str = os.environ["DUO_SMTP_PASS"]
SMTP_TIMEOUT_SECONDS: int = int(os.environ.get("DUO_SMTP_TIMEOUT_SECONDS", "8"))
SMTP_RETRIES: int = int(os.environ.get("DUO_SMTP_RETRIES", "0"))
SMTP_USE_SSL: bool = (
    os.environ.get("DUO_SMTP_USE_SSL", "").strip().lower() in {"1", "true", "yes", "on"}
    or SMTP_PORT == 465
)
BREVO_API_KEY: str = (
    os.environ.get("DUO_BREVO_API_KEY", "").strip()
    or os.environ.get("BREVO_API_KEY", "").strip()
)
BREVO_API_URL: str = os.environ.get(
    "DUO_BREVO_API_URL",
    "https://api.brevo.com/v3/smtp/email",
)


def _html_to_text(html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _send_via_brevo_api(
    *,
    subject: str,
    body: str,
    to_addr: str,
    from_addr: str | None = None,
) -> None:
    if not BREVO_API_KEY:
        raise RuntimeError("Brevo API key is not configured")

    sender_email = from_addr or "no-reply@duolicious.app"
    payload = {
        "sender": {
            "name": "JwBoo",
            "email": sender_email,
        },
        "to": [{"email": to_addr}],
        "replyTo": {
            "name": "JwBoo",
            "email": sender_email,
        },
        "subject": subject,
        "htmlContent": body,
        "textContent": _html_to_text(body),
    }

    request = urllib.request.Request(
        BREVO_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "accept": "application/json",
            "api-key": BREVO_API_KEY,
            "content-type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=SMTP_TIMEOUT_SECONDS) as response:
        status = getattr(response, "status", response.getcode())
        if status < 200 or status >= 300:
            raise RuntimeError(f"Brevo API send failed with status {status}")


def _should_login(username: str, password: str) -> bool:
    if not username or not password:
        return False

    # Dev docker-compose uses MailHog with placeholder credentials.
    if username == "unused-in-dev-env" or password == "unused-in-dev-env":
        return False

    return True


class Smtp:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        connect_immediately: bool = True,
    ) -> None:
        self.host: str = host
        self.port: int = port
        self.username: str = username
        self.password: str = password
        self._smtp: smtplib.SMTP | None = None

        self._lock: threading.RLock = threading.RLock()

        if connect_immediately:
            self._connect()

    def _connect(self) -> None:
        """(Re)‑establish an SMTP connection (protected by *lock*)."""
        with self._lock:
            if self._smtp is not None:
                try:
                    self._smtp.noop()
                    return  # connection still healthy
                except (smtplib.SMTPServerDisconnected, OSError):
                    self._smtp = None

            try:
                print(f"Establishing connection to SMTP server at {self.host}")
                if SMTP_USE_SSL:
                    smtp = smtplib.SMTP_SSL(self.host, self.port, timeout=SMTP_TIMEOUT_SECONDS)
                    smtp.ehlo()
                    print("SMTP SSL connection initiated.")
                else:
                    smtp = smtplib.SMTP(self.host, self.port, timeout=SMTP_TIMEOUT_SECONDS)
                    smtp.ehlo()

                    if smtp.has_extn("starttls"):
                        smtp.starttls()
                        smtp.ehlo()  # re-identify as TLS is now in effect
                        print("STARTTLS supported and initiated.")
                    else:
                        print("STARTTLS not supported by server.")

                if _should_login(self.username, self.password):
                    smtp.login(self.username, self.password)
                self._smtp = smtp
                print(f"Connection to SMTP server at {self.host} established")
            except Exception as exc:
                print(f"Failed to connect to SMTP server: {exc}")
                self._smtp = None
                raise

    def _try_send(
        self,
        *,
        subject: str,
        body: str,
        to_addr: str,
        from_addr: str | None = None,
    ) -> None:
        if self._smtp is None:
            # Lazily reconnect if previous attempt failed.
            self._connect()

        if self._smtp is None:
            raise Exception("Connection couldn't be established")

        _from_addr: str = from_addr or "no-reply@duolicious.app"

        msg = MIMEMultipart("alternative")
        msg["From"] = f"Duolicious <{_from_addr}>"
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        self._smtp.sendmail(
            from_addr=_from_addr,
            to_addrs=[to_addr],
            msg=msg.as_string(),
        )

    def send(
        self,
        *,
        subject: str,
        body: str,
        to_addr: str,
        from_addr: str | None = None,
        retries: int | None = None,
        backoff: int | None = None,
    ) -> None:
        """Send an email, retrying on failure.

        Back‑off doubles on every failed attempt: *backoff* × 2^(n - 1).
        """
        max_attempts: int = 1 + (SMTP_RETRIES if retries is None else retries)
        last_exception: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                with self._lock:
                    self._try_send(
                        subject=subject,
                        body=body,
                        to_addr=to_addr,
                        from_addr=from_addr,
                    )
                return  # Success
            except Exception as exc:
                last_exception = exc
                print(traceback.format_exc())
                if attempt == max_attempts:
                    print("All retry attempts exhausted. Giving up.")
                    break

                delay_base: float = 1.0 if backoff is None else backoff
                delay = delay_base * (2 ** (attempt - 1))
                print(f"Attempt {attempt} failed; retrying in {delay:.1f}s.")
                time.sleep(delay)

                # Best effort reconnect for the next iteration
                with suppress(Exception):
                    self._connect()

        if last_exception is not None:
            if BREVO_API_KEY:
                print("SMTP send failed; falling back to Brevo API.")
                _send_via_brevo_api(
                    subject=subject,
                    body=body,
                    to_addr=to_addr,
                    from_addr=from_addr,
                )
                return
            raise last_exception

    # ------------------------------------------------------------------

    def quit(self) -> None:
        """Explicitly close the SMTP connection."""
        with self._lock:
            if self._smtp is not None:
                try:
                    self._smtp.quit()
                except Exception as exc:
                    print(f"Error while quitting SMTP connection: {exc}")
                finally:
                    self._smtp = None

    def __del__(self) -> None:
        with suppress(Exception):  # _smtp may already be closed
            self.quit()


def make_aws_smtp() -> Smtp:
    return Smtp(
        SMTP_HOST,
        SMTP_PORT,
        SMTP_USER,
        SMTP_PASS,
        connect_immediately=False,
    )


aws_smtp: Smtp = make_aws_smtp()
