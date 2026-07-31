"""Single shared "today" for every data_generation script.

Regenerating data on a different calendar day must produce byte-identical
output under the same random seed. It's not enough to just fix an explicit
end_date="today" -- Faker also resolves relative strings like start_date="-10y"
and fake.date_of_birth()'s age range against the *real* wall clock internally,
which silently breaks reproducibility even when only a relative bound is used.
So every script:
  - imports REFERENCE_DATE / REFERENCE_DATETIME instead of using date.today()
    or datetime.now(), and
  - uses years_before(REFERENCE_DATE, N) + fake.date_between_dates(...) instead
    of fake.date_between(start_date="-Ny", ...) or fake.date_of_birth(...),
    since date_between_dates never consults datetime.now() when both bounds
    are given explicitly.
"""
from datetime import date, datetime, time

REFERENCE_DATE = date(2026, 7, 31)
REFERENCE_DATETIME = datetime.combine(REFERENCE_DATE, time.min)


def years_before(reference: date, years: int) -> date:
    """reference - N calendar years (Feb-29 falls back to Mar 1)."""
    try:
        return reference.replace(year=reference.year - years)
    except ValueError:
        return reference.replace(month=3, day=1, year=reference.year - years)
