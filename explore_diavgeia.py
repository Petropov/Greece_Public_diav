#!/usr/bin/env python3
import pandas as pd
import json
from datetime import datetime

# Load sample data
df = pd.read_json("output/decisions.jsonl", lines=True)

# Clean timestamps
df["submission"] = pd.to_datetime(df["submissionTimestamp"], unit="ms", errors="coerce")
df["publish"] = pd.to_datetime(df["publishTimestamp"], unit="ms", errors="coerce")
df["delay_days"] = (df["publish"] - df["submission"]).dt.total_seconds() / (3600 * 24)

# Display structure
print("Records:", len(df))
print(df.head(3)[["ada", "subject", "organizationName", "decisionTypeId", "delay_days"]])

