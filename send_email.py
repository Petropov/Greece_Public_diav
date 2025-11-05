#!/usr/bin/env python3
import os, smtplib
from email.mime.text import MIMEText
from email.header import Header
from datetime import date

to = os.environ["DIGEST_TO"]  # comma-separated addresses
sender = os.environ.get("DIGEST_FROM", os.environ.get("SMTP_USER"))
subj = os.environ.get("DIGEST_SUBJ") or f"Diavgeia Digest — {date.today().strftime('%B %Y')}"

digest_path = "artifacts/digest.html"
tpl_path = "templates/newsletter_template.html"

with open(digest_path, "r", encoding="utf-8") as f:
    digest_html = f.read()

if os.path.exists(tpl_path):
    with open(tpl_path, "r", encoding="utf-8") as f:
        html = f.read().replace("{{DIGEST_HTML}}", digest_html)
else:
    html = digest_html

msg = MIMEText(html, "html", "utf-8")
msg["Subject"] = str(Header(subj, "utf-8"))
msg["From"] = sender
msg["To"] = to

host = os.environ["SMTP_HOST"]
port = int(os.environ.get("SMTP_PORT", "587"))
user = os.environ["SMTP_USER"]
pwd  = os.environ["SMTP_PASS"]

with smtplib.SMTP(host, port) as s:
    s.starttls()
    s.login(user, pwd)
    s.sendmail(sender, [x.strip() for x in to.split(",") if x.strip()], msg.as_string())

print("Sent.")
