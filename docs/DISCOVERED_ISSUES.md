# Discovered Issues

Data quality issues found by the validator suite that were **not** deliberately
seeded by `data_generation/seed_defects.py`. Those intentional defects are
tracked in `data_generation/defects_manifest.json`; this file is for real
issues the validators caught on their own, documented for transparency
rather than silently regenerated away.

## Claims dated before their policy's start date

- **Issue:** 2,856 of 9,070 claims (~31.5%) have a `claim_date` earlier than
  their related policy's `policy_start_date`.

- **Root cause:** `generate_claims.py` and `generate_policies.py` generate
  their date ranges independently -- claims draw from the last 3 years,
  policies from the last 5 years -- with no cross-table awareness. Each
  `claim_date` and `policy_start_date` value is drawn randomly from its own
  table's range with no constraint relative to the other, so a claim can
  easily land earlier than the policy it's filed against.

- **How it was found:** `framework/validators/date_validator.py`'s
  cross-table check (`claims` -> `policies`, comparing `claim_date` against
  the parent policy's `policy_start_date`), which was built specifically to
  validate this kind of real-world temporal consistency.

- **Decision:** left as-is in the dataset rather than regenerated. It
  demonstrates the validator catching a genuine, unplanted data quality
  issue in generated data -- exactly the kind of thing this framework is
  meant to catch -- so it's documented here instead of quietly fixed.

- **Status:** `date_validator.py` correctly reports the `claims` table as
  failed for this check. This is expected and correct validator behavior,
  not a bug in the validator.

## pipeline/04_deequ_checks.py cannot run on this workspace

- **Issue:** `pipeline/04_deequ_checks.py` was fully built and code-reviewed,
  but cannot be executed on this Databricks Free Edition workspace.

- **Root cause:** the workspace only offers Serverless compute, and
  Serverless notebooks do not support JAR libraries. `pydeequ` is just a
  Python API wrapper -- the actual Deequ engine is a JVM library
  (`com.amazon.deequ:deequ:<version>`) that must be loaded into the Spark
  session as a JAR, which Serverless has no mechanism to attach. This is a
  platform-level restriction confirmed via Databricks' own documentation,
  not a bug in our code.

- **How it was found:** attempting to plan a live run of
  `04_deequ_checks.py` against this workspace's Serverless compute.

- **Decision:** the file stays in the repo as-is. It demonstrates the
  intended Deequ API design (`VerificationSuite`, `Check` constraints built
  from `mandatory_columns`/`primary_key`/`business_rules`,
  `ConstraintSuggestionRunner` profiling) even though it can't run here.
  Great Expectations, run via SQL (which Serverless does support), serves
  as this project's actual functioning statistical/profiling data-quality
  tool instead.

- **Status:** `04_deequ_checks.py` is unexecuted on this workspace by
  platform limitation, not by validator or code defect. Treat it as
  reference/demonstration code unless run on a workspace with
  JAR-cluster/classic compute available.
