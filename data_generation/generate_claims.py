"""Generates clean claims data into landing/claims/ (two batches).

Depends on landing/policies/*.csv already existing (policy_id FK pool, and
policy_type used to pick a flavor-appropriate claim_reason).

Batch 2 deliberately includes some late-arriving claims (claim_date earlier
than batch 1's earliest date) -- that's an intentional timing pattern to
test on, not a defect. Actual defects (orphan FKs, nulls, negative amounts,
duplicates) are injected later in seed_defects.py.
"""
from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

from reference_date import REFERENCE_DATE, REFERENCE_DATETIME, years_before

SEED = 42
POLICIES_DIR = Path(__file__).resolve().parent.parent / "landing" / "policies"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "landing" / "claims"

CORE_HEADERS = [
    "claim_id",
    "policy_id",
    "claim_date",
    "claim_amount",
    "claim_status",
    "claim_reason",
]
FULL_HEADERS = CORE_HEADERS + ["batch_id", "load_timestamp"]

CLAIM_STATUSES = ["Approved", "Denied", "Pending"]

CLAIM_REASONS_BY_POLICY_TYPE = {
    "Auto": ["Vehicle collision", "Windshield damage", "Theft of vehicle", "Hit and run", "Vandalism damage"],
    "Home": ["Water damage", "Fire damage", "Roof damage from storm", "Burglary", "Fallen tree damage"],
    "Health": ["Medical procedure", "Emergency room visit", "Surgery reimbursement", "Prescription claim", "Hospital stay"],
    "Life": ["Death benefit claim", "Terminal illness claim", "Accidental death claim"],
}

BATCH1_COUNT = 7000
BATCH2_COUNT = 2000
BATCH2_LATE_COUNT = 300


def make_claim_id(n: int) -> str:
    return f"CLM{n:06d}"


def load_valid_policies() -> tuple[list[str], dict[str, str]]:
    """Read both policy batches; return (ordered unique policy_ids, policy_id -> policy_type)."""
    seen: set[str] = set()
    ordered_ids: list[str] = []
    policy_type_by_id: dict[str, str] = {}
    for filename in ("policies_batch1.csv", "policies_batch2.csv"):
        path = POLICIES_DIR / filename
        with open(path, "r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                policy_id = row["policy_id"]
                policy_type_by_id[policy_id] = row["policy_type"]
                if policy_id not in seen:
                    seen.add(policy_id)
                    ordered_ids.append(policy_id)
    return ordered_ids, policy_type_by_id


def generate_claim_fields(policy_id: str, claim_date: date, policy_type_by_id: dict[str, str]) -> dict:
    policy_type = policy_type_by_id.get(policy_id, "Auto")
    claim_amount = round(random.uniform(100, 25000), 2)
    return {
        "policy_id": policy_id,
        "claim_date": claim_date.isoformat(),
        "claim_amount": f"{claim_amount:.2f}",
        "claim_status": random.choice(CLAIM_STATUSES),
        "claim_reason": random.choice(CLAIM_REASONS_BY_POLICY_TYPE[policy_type]),
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

    valid_policy_ids, policy_type_by_id = load_valid_policies()

    # --- Batch 1: 7000 fresh claims, dates sorted so the file reads chronologically ---
    batch1_timestamp = REFERENCE_DATETIME.isoformat(timespec="seconds")

    claims_range_start = years_before(REFERENCE_DATE, 3)
    batch1_dates = sorted(
        fake.date_between_dates(date_start=claims_range_start, date_end=REFERENCE_DATE) for _ in range(BATCH1_COUNT)
    )
    earliest_batch1_date = batch1_dates[0]

    batch1_rows = []
    for i, claim_date in enumerate(batch1_dates, start=1):
        policy_id = random.choice(valid_policy_ids)
        row = {
            "claim_id": make_claim_id(i),
            **generate_claim_fields(policy_id, claim_date, policy_type_by_id),
            "batch_id": "batch_001",
            "load_timestamp": batch1_timestamp,
        }
        batch1_rows.append(row)

    batch1_path = OUTPUT_DIR / "claims_batch1.csv"
    write_csv(batch1_path, batch1_rows)

    # --- Batch 2: 2000 new claims -- 300 late-arriving + 1700 normal recent ---
    batch2_timestamp = (
        datetime.fromisoformat(batch1_timestamp) + timedelta(days=random.randint(2, 5))
    ).isoformat(timespec="seconds")

    late_flags = [True] * BATCH2_LATE_COUNT + [False] * (BATCH2_COUNT - BATCH2_LATE_COUNT)
    random.shuffle(late_flags)

    batch2_rows = []
    late_count = 0
    for offset, is_late in enumerate(late_flags):
        i = BATCH1_COUNT + 1 + offset
        policy_id = random.choice(valid_policy_ids)
        if is_late:
            claim_date = earliest_batch1_date - timedelta(days=random.randint(1, 180))
            late_count += 1
        else:
            claim_date = fake.date_between_dates(date_start=claims_range_start, date_end=REFERENCE_DATE)

        row = {
            "claim_id": make_claim_id(i),
            **generate_claim_fields(policy_id, claim_date, policy_type_by_id),
            "batch_id": "batch_002",
            "load_timestamp": batch2_timestamp,
        }
        batch2_rows.append(row)

    batch2_path = OUTPUT_DIR / "claims_batch2.csv"
    write_csv(batch2_path, batch2_rows)

    valid_set = set(valid_policy_ids)
    all_policy_ids_valid = all(row["policy_id"] in valid_set for row in batch1_rows + batch2_rows)

    print("=== generate_claims.py summary ===")
    print(f"Valid policy_id pool size: {len(valid_policy_ids)}")
    print(
        f"Batch 1: {len(batch1_rows)} rows -> {batch1_path} "
        f"(earliest claim_date: {earliest_batch1_date.isoformat()})"
    )
    print(f"Batch 2: {len(batch2_rows)} rows -> {batch2_path}")
    print(f"  Late-arriving (claim_date before batch 1's earliest date): {late_count}")
    print(f"  Normal recent-dated: {len(batch2_rows) - late_count}")
    print(f"All policy_id values used exist in valid pool: {all_policy_ids_valid}")


if __name__ == "__main__":
    main()
