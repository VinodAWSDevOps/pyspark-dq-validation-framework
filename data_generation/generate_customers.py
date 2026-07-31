"""Generates clean customers data into landing/customers/ (two batches).

No defects (nulls, dupes) here by design -- seed_defects.py injects those later.
"""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from reference_date import REFERENCE_DATE, REFERENCE_DATETIME, years_before

SEED = 42
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "landing" / "customers"

CORE_HEADERS = ["customer_id", "customer_name", "dob", "address", "email", "customer_since"]
FULL_HEADERS = CORE_HEADERS + ["batch_id", "load_timestamp"]

BATCH1_COUNT = 2000
BATCH2_CHANGED_COUNT = 150
BATCH2_NEW_COUNT = 50


def make_customer_id(n: int) -> str:
    return f"CUST{n:05d}"


def generate_customer_fields(fake: Faker) -> dict:
    dob = fake.date_between_dates(
        date_start=years_before(REFERENCE_DATE, 85), date_end=years_before(REFERENCE_DATE, 18)
    )
    customer_since = fake.date_between_dates(
        date_start=years_before(REFERENCE_DATE, 10), date_end=REFERENCE_DATE - timedelta(days=1)
    )
    return {
        "customer_name": fake.name(),
        "dob": dob.isoformat(),
        "address": fake.address().replace("\n", ", "),
        "email": fake.unique.email(),
        "customer_since": customer_since.isoformat(),
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

    # --- Batch 1: 2000 fresh customers ---
    batch1_timestamp = REFERENCE_DATETIME.isoformat(timespec="seconds")
    batch1_rows = []
    for i in range(1, BATCH1_COUNT + 1):
        row = {
            "customer_id": make_customer_id(i),
            **generate_customer_fields(fake),
            "batch_id": "batch_001",
            "load_timestamp": batch1_timestamp,
        }
        batch1_rows.append(row)

    batch1_path = OUTPUT_DIR / "customers_batch1.csv"
    write_csv(batch1_path, batch1_rows)

    # --- Batch 2: 150 changed existing customers + 50 brand new ---
    batch2_timestamp = (
        datetime.fromisoformat(batch1_timestamp) + timedelta(days=random.randint(2, 5))
    ).isoformat(timespec="seconds")

    changed_rows = []
    for original in random.sample(batch1_rows, BATCH2_CHANGED_COUNT):
        updated = dict(original)
        change_target = random.choice(["address", "email", "both"])
        if change_target in ("address", "both"):
            updated["address"] = fake.address().replace("\n", ", ")
        if change_target in ("email", "both"):
            updated["email"] = fake.unique.email()
        updated["batch_id"] = "batch_002"
        updated["load_timestamp"] = batch2_timestamp
        changed_rows.append(updated)

    new_rows = []
    for i in range(BATCH1_COUNT + 1, BATCH1_COUNT + BATCH2_NEW_COUNT + 1):
        row = {
            "customer_id": make_customer_id(i),
            **generate_customer_fields(fake),
            "batch_id": "batch_002",
            "load_timestamp": batch2_timestamp,
        }
        new_rows.append(row)

    batch2_rows = changed_rows + new_rows
    batch2_path = OUTPUT_DIR / "customers_batch2.csv"
    write_csv(batch2_path, batch2_rows)

    print("=== generate_customers.py summary ===")
    print(f"Batch 1: {len(batch1_rows)} rows -> {batch1_path}")
    print(f"Batch 2: {len(batch2_rows)} rows -> {batch2_path}")
    print(f"  Changed (existing customer_id, new address/email): {len(changed_rows)}")
    print(f"  New customers: {len(new_rows)}")


if __name__ == "__main__":
    main()
