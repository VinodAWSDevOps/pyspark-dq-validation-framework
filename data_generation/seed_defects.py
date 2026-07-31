"""Injects intentional data-quality defects into new landing/ files.

Never touches existing batch1/batch2 files -- only adds new ones, plus
defects_manifest.json enumerating every defect row for test authoring.

Reuses the field generators from generate_customers.py / generate_policies.py
/ generate_claims.py so defect rows look like realistic data with exactly
one thing wrong, rather than hand-rolled fixtures.
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

from generate_customers import (
    FULL_HEADERS as CUSTOMER_HEADERS,
    generate_customer_fields,
    make_customer_id,
)
from generate_policies import (
    FULL_HEADERS as POLICY_HEADERS,
    generate_policy_fields,
    load_valid_customer_ids,
    make_policy_id,
)
from generate_claims import (
    FULL_HEADERS as CLAIM_HEADERS,
    generate_claim_fields,
    load_valid_policies,
    make_claim_id,
)
from reference_date import REFERENCE_DATE, years_before

SEED = 42
REPO_ROOT = Path(__file__).resolve().parent.parent
LANDING = REPO_ROOT / "landing"
MANIFEST_PATH = Path(__file__).resolve().parent / "defects_manifest.json"

DEFECTS_BATCH_ID = "batch_003_defects"
SCHEMA_DRIFT_BATCH_ID = "batch_004_schema_drift"

manifest_entries: list[dict] = []


def log_defect(table: str, file_path: Path, row_identifier: str, defect_type: str, expected_validator: str) -> None:
    manifest_entries.append(
        {
            "table": table,
            "file": str(file_path.relative_to(REPO_ROOT)).replace("\\", "/"),
            "row_identifier": row_identifier,
            "defect_type": defect_type,
            "expected_validator": expected_validator,
        }
    )


def read_rows(path: Path) -> list[dict]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_batch_timestamp(path: Path) -> str:
    return read_rows(path)[0]["load_timestamp"]


def offset_timestamp(timestamp: str, min_days: int = 2, max_days: int = 5) -> str:
    from datetime import datetime

    dt = datetime.fromisoformat(timestamp) + timedelta(days=random.randint(min_days, max_days))
    return dt.isoformat(timespec="seconds")


def write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def fake_id(prefix: str) -> str:
    """An id that looks real but is guaranteed out of range of any real pool used in this project."""
    return f"{prefix}{random.randint(90000, 99999)}"


# ---------------------------------------------------------------- customers

def build_customers_defects(fake: Faker, timestamp: str) -> list[dict]:
    file_path = LANDING / "customers" / "customers_batch3_defects.csv"
    rows: list[dict] = []
    next_id = 2051

    for _ in range(10):  # NULL customer_name
        customer_id = make_customer_id(next_id)
        next_id += 1
        fields = generate_customer_fields(fake)
        fields["customer_name"] = ""
        rows.append({"customer_id": customer_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("customers", file_path, customer_id, "null_customer_name", "completeness_validator")

    for _ in range(10):  # NULL dob
        customer_id = make_customer_id(next_id)
        next_id += 1
        fields = generate_customer_fields(fake)
        fields["dob"] = ""
        rows.append({"customer_id": customer_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("customers", file_path, customer_id, "null_dob", "completeness_validator")

    batch1_rows = read_rows(LANDING / "customers" / "customers_batch1.csv")
    for original in random.sample(batch1_rows, 10):  # duplicate customer_id
        duplicate = {**original, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp}
        rows.append(duplicate)
        log_defect("customers", file_path, duplicate["customer_id"], "duplicate_customer_id", "uniqueness_validator")

    for _ in range(10):  # future dob
        customer_id = make_customer_id(next_id)
        next_id += 1
        fields = generate_customer_fields(fake)
        fields["dob"] = (REFERENCE_DATE + timedelta(days=random.randint(1, 3650))).isoformat()
        rows.append({"customer_id": customer_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("customers", file_path, customer_id, "future_dob", "business_rule_validator")

    write_csv(file_path, CUSTOMER_HEADERS, rows)
    return rows


# ----------------------------------------------------------------- policies

def build_policies_defects(fake: Faker, valid_customer_ids: list[str], timestamp: str) -> list[dict]:
    file_path = LANDING / "policies" / "policies_batch3_defects.csv"
    rows: list[dict] = []
    next_id = 5101

    for _ in range(15):  # orphan customer_id
        policy_id = make_policy_id(next_id)
        next_id += 1
        fields = generate_policy_fields(fake, valid_customer_ids)
        fields["customer_id"] = fake_id("CUST")
        rows.append({"policy_id": policy_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("policies", file_path, policy_id, "orphan_customer_id", "referential_integrity_validator")

    for _ in range(15):  # negative premium_amount
        policy_id = make_policy_id(next_id)
        next_id += 1
        fields = generate_policy_fields(fake, valid_customer_ids)
        fields["premium_amount"] = f"{-round(random.uniform(50, 5000), 2):.2f}"
        rows.append({"policy_id": policy_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("policies", file_path, policy_id, "negative_premium_amount", "business_rule_validator")

    for _ in range(15):  # end_date before start_date
        policy_id = make_policy_id(next_id)
        next_id += 1
        fields = generate_policy_fields(fake, valid_customer_ids)
        start = date.fromisoformat(fields["policy_start_date"])
        fields["policy_end_date"] = (start - timedelta(days=random.randint(1, 365))).isoformat()
        rows.append({"policy_id": policy_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("policies", file_path, policy_id, "end_date_before_start_date", "business_rule_validator")

    batch1_rows = read_rows(LANDING / "policies" / "policies_batch1.csv")
    for original in random.sample(batch1_rows, 15):  # duplicate policy_id
        duplicate = {**original, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp}
        rows.append(duplicate)
        log_defect("policies", file_path, duplicate["policy_id"], "duplicate_policy_id", "uniqueness_validator")

    write_csv(file_path, POLICY_HEADERS, rows)
    return rows


# ------------------------------------------------------------------- claims

def build_claims_defects(
    fake: Faker, valid_policy_ids: list[str], policy_type_by_id: dict[str, str], timestamp: str
) -> list[dict]:
    file_path = LANDING / "claims" / "claims_batch3_defects.csv"
    rows: list[dict] = []
    next_id = 9001

    def random_recent_date() -> date:
        return fake.date_between_dates(date_start=years_before(REFERENCE_DATE, 3), date_end=REFERENCE_DATE)

    for _ in range(20):  # orphan policy_id
        claim_id = make_claim_id(next_id)
        next_id += 1
        fields = generate_claim_fields(random.choice(valid_policy_ids), random_recent_date(), policy_type_by_id)
        fields["policy_id"] = fake_id("POL")
        rows.append({"claim_id": claim_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("claims", file_path, claim_id, "orphan_policy_id", "referential_integrity_validator")

    for _ in range(20):  # NULL claim_amount
        claim_id = make_claim_id(next_id)
        next_id += 1
        fields = generate_claim_fields(random.choice(valid_policy_ids), random_recent_date(), policy_type_by_id)
        fields["claim_amount"] = ""
        rows.append({"claim_id": claim_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("claims", file_path, claim_id, "null_claim_amount", "completeness_validator")

    for _ in range(15):  # negative claim_amount
        claim_id = make_claim_id(next_id)
        next_id += 1
        fields = generate_claim_fields(random.choice(valid_policy_ids), random_recent_date(), policy_type_by_id)
        fields["claim_amount"] = f"{-round(random.uniform(50, 25000), 2):.2f}"
        rows.append({"claim_id": claim_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("claims", file_path, claim_id, "negative_claim_amount", "business_rule_validator")

    for _ in range(15):  # invalid claim_status enum
        claim_id = make_claim_id(next_id)
        next_id += 1
        fields = generate_claim_fields(random.choice(valid_policy_ids), random_recent_date(), policy_type_by_id)
        fields["claim_status"] = "Escalated"
        rows.append({"claim_id": claim_id, **fields, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp})
        log_defect("claims", file_path, claim_id, "invalid_claim_status_enum", "business_rule_validator")

    batch1_rows = read_rows(LANDING / "claims" / "claims_batch1.csv")
    for original in random.sample(batch1_rows, 10):  # duplicate claim_id
        duplicate = {**original, "batch_id": DEFECTS_BATCH_ID, "load_timestamp": timestamp}
        rows.append(duplicate)
        log_defect("claims", file_path, duplicate["claim_id"], "duplicate_claim_id", "uniqueness_validator")

    write_csv(file_path, CLAIM_HEADERS, rows)
    return rows


def build_claims_schema_drift(
    fake: Faker, valid_policy_ids: list[str], policy_type_by_id: dict[str, str], timestamp: str
) -> list[dict]:
    file_path = LANDING / "claims" / "claims_batch4_schema_drift.csv"
    rows: list[dict] = []
    start_id = 9081

    for offset in range(20):
        claim_id = make_claim_id(start_id + offset)
        claim_date = fake.date_between_dates(date_start=years_before(REFERENCE_DATE, 3), date_end=REFERENCE_DATE)
        fields = generate_claim_fields(random.choice(valid_policy_ids), claim_date, policy_type_by_id)
        row = {"claim_id": claim_id, **fields, "batch_id": SCHEMA_DRIFT_BATCH_ID, "load_timestamp": timestamp}
        row["reason_notes"] = row.pop("claim_reason")
        rows.append(row)
        log_defect("claims", file_path, claim_id, "schema_drift_renamed_column", "file_format_validator")

    drifted_headers = [h if h != "claim_reason" else "reason_notes" for h in CLAIM_HEADERS]
    write_csv(file_path, drifted_headers, rows)
    return rows


def main() -> None:
    Faker.seed(SEED)
    random.seed(SEED)
    fake = Faker()

    customers_ts = offset_timestamp(read_batch_timestamp(LANDING / "customers" / "customers_batch2.csv"))
    policies_ts = offset_timestamp(read_batch_timestamp(LANDING / "policies" / "policies_batch2.csv"))
    claims_ts = offset_timestamp(read_batch_timestamp(LANDING / "claims" / "claims_batch2.csv"))
    schema_drift_ts = offset_timestamp(claims_ts)

    customer_rows = build_customers_defects(fake, customers_ts)

    valid_customer_ids = load_valid_customer_ids()
    policy_rows = build_policies_defects(fake, valid_customer_ids, policies_ts)

    valid_policy_ids, policy_type_by_id = load_valid_policies()
    claim_rows = build_claims_defects(fake, valid_policy_ids, policy_type_by_id, claims_ts)
    schema_drift_rows = build_claims_schema_drift(fake, valid_policy_ids, policy_type_by_id, schema_drift_ts)

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest_entries, f, indent=2)

    print("=== seed_defects.py summary ===")
    print(f"customers: {len(customer_rows)} defect rows")
    print(f"policies:  {len(policy_rows)} defect rows")
    print(f"claims:    {len(claim_rows) + len(schema_drift_rows)} defect rows "
          f"({len(claim_rows)} in batch3_defects, {len(schema_drift_rows)} in batch4_schema_drift)")
    print(f"Total defect rows logged in manifest: {len(manifest_entries)}")

    print("\nBy defect_type:")
    counts_by_type: dict[str, int] = {}
    for entry in manifest_entries:
        counts_by_type[entry["defect_type"]] = counts_by_type.get(entry["defect_type"], 0) + 1
    for defect_type, count in sorted(counts_by_type.items()):
        print(f"  {defect_type}: {count}")

    print(f"\nManifest written to: {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
