"""Generates clean policies data into landing/policies/ (two batches).

Depends on landing/customers/*.csv already existing (customer_id FK pool).
No defects (orphan FKs, bad dates, negative amounts) here -- seed_defects.py
injects those later.
"""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from reference_date import REFERENCE_DATE, REFERENCE_DATETIME, years_before

SEED = 42
CUSTOMERS_DIR = Path(__file__).resolve().parent.parent / "landing" / "customers"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "landing" / "policies"

CORE_HEADERS = [
    "policy_id",
    "customer_id",
    "policy_type",
    "policy_start_date",
    "policy_end_date",
    "premium_amount",
    "policy_status",
]
FULL_HEADERS = CORE_HEADERS + ["batch_id", "load_timestamp"]

POLICY_TYPES = ["Auto", "Home", "Health", "Life"]
POLICY_STATUSES = ["Active", "Lapsed", "Cancelled"]

BATCH1_COUNT = 5000
BATCH2_CHANGED_COUNT = 300
BATCH2_NEW_COUNT = 100


def make_policy_id(n: int) -> str:
    return f"POL{n:05d}"


def load_valid_customer_ids() -> list[str]:
    """Read both customer batches and return deduped customer_id values."""
    seen: set[str] = set()
    ordered_ids: list[str] = []
    for filename in ("customers_batch1.csv", "customers_batch2.csv"):
        path = CUSTOMERS_DIR / filename
        with open(path, "r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                customer_id = row["customer_id"]
                if customer_id not in seen:
                    seen.add(customer_id)
                    ordered_ids.append(customer_id)
    return ordered_ids


def generate_policy_fields(fake: Faker, valid_customer_ids: list[str]) -> dict:
    start_date = fake.date_between_dates(date_start=years_before(REFERENCE_DATE, 5), date_end=REFERENCE_DATE)
    end_date = start_date + timedelta(days=random.randint(1, 3) * 365)
    premium_amount = round(random.uniform(200, 5000), 2)
    return {
        "customer_id": random.choice(valid_customer_ids),
        "policy_type": random.choice(POLICY_TYPES),
        "policy_start_date": start_date.isoformat(),
        "policy_end_date": end_date.isoformat(),
        "premium_amount": f"{premium_amount:.2f}",
        "policy_status": random.choice(POLICY_STATUSES),
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FULL_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    Faker.seed(SEED)
    random.seed(SEED)
    fake = Faker()

    valid_customer_ids = load_valid_customer_ids()

    # --- Batch 1: 5000 fresh policies ---
    batch1_timestamp = REFERENCE_DATETIME.isoformat(timespec="seconds")
    batch1_rows = []
    for i in range(1, BATCH1_COUNT + 1):
        row = {
            "policy_id": make_policy_id(i),
            **generate_policy_fields(fake, valid_customer_ids),
            "batch_id": "batch_001",
            "load_timestamp": batch1_timestamp,
        }
        batch1_rows.append(row)

    batch1_path = OUTPUT_DIR / "policies_batch1.csv"
    write_csv(batch1_path, batch1_rows)

    # --- Batch 2: 300 changed existing policies + 100 brand new ---
    batch2_timestamp = (
        datetime.fromisoformat(batch1_timestamp) + timedelta(days=random.randint(2, 5))
    ).isoformat(timespec="seconds")

    changed_rows = []
    for original in random.sample(batch1_rows, BATCH2_CHANGED_COUNT):
        updated = dict(original)
        change_target = random.choice(["premium_amount", "policy_status", "both"])
        if change_target in ("premium_amount", "both"):
            updated["premium_amount"] = f"{round(random.uniform(200, 5000), 2):.2f}"
        if change_target in ("policy_status", "both"):
            updated["policy_status"] = random.choice(
                [status for status in POLICY_STATUSES if status != original["policy_status"]]
            )
        updated["batch_id"] = "batch_002"
        updated["load_timestamp"] = batch2_timestamp
        changed_rows.append(updated)

    new_rows = []
    for i in range(BATCH1_COUNT + 1, BATCH1_COUNT + BATCH2_NEW_COUNT + 1):
        row = {
            "policy_id": make_policy_id(i),
            **generate_policy_fields(fake, valid_customer_ids),
            "batch_id": "batch_002",
            "load_timestamp": batch2_timestamp,
        }
        new_rows.append(row)

    batch2_rows = changed_rows + new_rows
    batch2_path = OUTPUT_DIR / "policies_batch2.csv"
    write_csv(batch2_path, batch2_rows)

    valid_set = set(valid_customer_ids)
    all_customer_ids_valid = all(
        row["customer_id"] in valid_set for row in batch1_rows + batch2_rows
    )

    print("=== generate_policies.py summary ===")
    print(f"Valid customer_id pool size: {len(valid_customer_ids)}")
    print(f"Batch 1: {len(batch1_rows)} rows -> {batch1_path}")
    print(f"Batch 2: {len(batch2_rows)} rows -> {batch2_path}")
    print(f"  Changed (existing policy_id, new premium/status): {len(changed_rows)}")
    print(f"  New policies: {len(new_rows)}")
    print(f"All customer_id values used exist in valid pool: {all_customer_ids_valid}")


if __name__ == "__main__":
    main()
