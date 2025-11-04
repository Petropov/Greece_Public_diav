#!/usr/bin/env python3
import os, smtplib, sys
from email.mime.text import MIMEText
from datetime import date

to = os.environ["DIGEST_TO"]            # comma-separated addresses
sender = os.environ.get("DIGEST_FROM", "diav-digest@yourdomain")
subj = os.environ.get("DIGEST_SUBJ") or f"Diavgeia Digest — {date.today().strftime('%B %Y')}"
path = "artifacts/digest.html"

with open(path, "r", encoding="utf-8") as f:
    html = f.read()

msg = MIMEText(html, "html", "utf-8")
msg["Subject"] = subj
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
