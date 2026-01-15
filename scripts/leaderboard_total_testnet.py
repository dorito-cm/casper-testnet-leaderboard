#!/usr/bin/env python3
import csv
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, getcontext

import requests

# ---------- Config ----------
API_BASE = os.getenv("CSPR_CLOUD_BASE", "https://api.testnet.cspr.cloud").rstrip("/")
API_KEY = os.getenv("CSPR_CLOUD_KEY", "").strip()

# Input: a text file containing one public key per line
INPUT_KEYS_FILE = os.getenv("INPUT_KEYS_FILE", "public_keys.txt")

# Output files
CSV_OUT = os.getenv("CSV_OUT", "leaderboard_total_testnet.csv")
JSON_OUT = os.getenv("JSON_OUT", "leaderboard_total_testnet.json")

# Optional: limit number of accounts processed (0/empty means all)
LIMIT = int(os.getenv("LIMIT", "0") or "0")

# Requests
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "0.08"))

# Decimal precision for motes -> cspr formatting
getcontext().prec = 40

# ---------- Helpers ----------
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def motes_to_cspr_str(motes: int) -> str:
    # 1 CSPR = 10^9 motes
    return f"{(Decimal(motes) / Decimal(10**9)):.9f}"

def short_pk(pk: str) -> str:
    if not pk:
        return ""
    if len(pk) <= 14:
        return pk
    return f"{pk[:8]}…{pk[-6:]}"

def api_headers():
    if not API_KEY:
        return {"accept": "application/json"}
    return {
        "authorization": API_KEY,
        "accept": "application/json",
    }

def http_get_json(url: str):
    r = requests.get(url, headers=api_headers(), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def read_public_keys(path: str):
    keys = []
    if not os.path.exists(path):
        raise SystemExit(
            f"❌ Missing input file: {path}\n"
            f"Create it with one public key per line."
        )
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            k = line.strip()
            if not k:
                continue
            if k.startswith("#"):
                continue
            keys.append(k)
    return keys

def get_account(pk: str):
    url = f"{API_BASE}/accounts/{pk}"
    return http_get_json(url)["data"]

def get_delegations(pk: str, limit=200):
    # returns list of delegations with stake in motes
    url = f"{API_BASE}/accounts/{pk}/delegations?limit={limit}"
    j = http_get_json(url)
    return j.get("data", [])

def cspr_live_url(pk: str) -> str:
    # testnet link (important!)
    return f"https://testnet.cspr.live/account/{pk}"

# ---------- Main ----------
def main():
    public_keys = read_public_keys(INPUT_KEYS_FILE)
    if LIMIT and LIMIT > 0:
        public_keys = public_keys[:LIMIT]

    rows = []
    errors = []

    for i, pk in enumerate(public_keys, start=1):
        try:
            acct = get_account(pk)

            # liquid balance motes
            liquid_motes = int(acct.get("balance", "0"))

            # staked motes (sum of all delegations stakes)
            delegations = get_delegations(pk)
            staked_motes = 0
            for d in delegations:
                try:
                    staked_motes += int(d.get("stake", "0"))
                except Exception:
                    pass

            total_motes = liquid_motes + staked_motes

            row = {
                "rank": 0,  # filled after sorting
                "public_key_short": short_pk(pk),

                "total_cspr": motes_to_cspr_str(total_motes),
                "liquid_cspr": motes_to_cspr_str(liquid_motes),
                "staked_cspr": motes_to_cspr_str(staked_motes),

                "public_key": pk,
                "cspr_live_url": cspr_live_url(pk),

                "total_motes": str(total_motes),
                "liquid_motes": str(liquid_motes),
                "staked_motes": str(staked_motes),
            }

            rows.append(row)

        except Exception as e:
            errors.append({"public_key": pk, "error": str(e)})

        time.sleep(SLEEP_BETWEEN)

    # Sort by total_motes descending
    rows.sort(key=lambda r: int(r["total_motes"]), reverse=True)

    # Fill rank
    for idx, r in enumerate(rows, start=1):
        r["rank"] = idx

    # CSV
    fieldnames = [
        "rank",
        "public_key_short",
        "total_cspr",
        "liquid_cspr",
        "staked_cspr",
        "public_key",
        "cspr_live_url",
        "total_motes",
        "liquid_motes",
        "staked_motes",
    ]

    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # JSON
    out = {
        "network": "testnet",
        "updated_at": iso_now(),
        "rows": rows,
        "errors": errors,
    }
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"✅ Done. Wrote {CSV_OUT} and {JSON_OUT}.")
    if errors:
        print("⚠️ Some keys failed:")
        for e in errors[:25]:
            print("-", e["public_key"], "=>", e["error"])
        if len(errors) > 25:
            print(f"...and {len(errors)-25} more.")

if __name__ == "__main__":
    main()
