# Healthcare Member Enrollment ETL

A config-driven ETL pipeline that migrates member enrollment data out of a messy
legacy export, validates it against declarative rules, quarantines what fails
with a reason code, loads what passes into a warehouse table, and produces a
reconciliation report proving nothing went missing.

Written in pure Python with no third-party dependencies. Clone it and run it.

```bash
python -m etl.pipeline
python -m unittest discover -s tests
```

```
Read 20 rows | loaded 10 | quarantined 10
Reconciliation report written to output/reconciliation.md
```

## Why this exists

Most ETL examples move clean data between two systems that already agree with
each other. Real migrations are not that. The source is a decade-old export
nobody documented, half the dates are in three different formats, status codes
are single letters that mean something only to the retired system, and the
business will ask you to prove that every row is either loaded or accounted
for. This project models that problem instead of the easy one.

The data here is synthetic. The failure modes are not: every bad row in
`data/raw/members_legacy.csv` is a category of defect I have actually had to
chase down in production.

## How it works

```
data/raw/members_legacy.csv
        |
     extract          stream rows, never load the whole file into memory
        |
    transform         strip, case-normalize, de-currency (config-driven)
        |
    validate          type coercion + field rules + cross-field rules
        |
   +----+----+
   |         |
 accept   quarantine  reason code + the original raw row, as JSON
   |         |
  load       |        idempotent upsert on the natural key
   |         |
   +----+----+
        |
   reconcile          row counts, control totals, balance check
        |
output/reconciliation.md
```

### The mapping lives in config, not in code

`config/members_mapping.json` defines every field: where it comes from, what it
becomes, which transforms to apply, and which rules it must satisfy.

```json
{
  "source": "DOB",
  "target": "date_of_birth",
  "type": "date",
  "transforms": ["strip"],
  "required": true,
  "input_formats": ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y", "%m/%d/%y"],
  "not_future": true
}
```

Adding a field, accepting a fourth date format, or widening an allowed-value
set is a config change. That is the difference between tooling you can hand to
someone else and a script only its author can maintain. The second source
system is what tells you whether you built the right thing, so this was
designed as though it were already coming.

### Bad rows are quarantined, not dropped and not fatal

A migration that aborts on the first malformed row never finishes. A migration
that silently skips rows is worse, because the counts look fine until someone
notices a member is missing. So every rejected row goes to a `quarantine` table
with a machine-readable reason code, the offending field, the bad value, and
the complete original row as JSON.

| Reason code | Meaning |
| --- | --- |
| `REQUIRED_MISSING` | Required field was empty |
| `PATTERN_MISMATCH` | Value did not match the expected format |
| `BAD_DATE` | Value could not be parsed as a date |
| `FUTURE_DATE` | Date is in the future |
| `BAD_DECIMAL` | Value could not be parsed as a number |
| `OUT_OF_RANGE` | Numeric value fell outside the allowed range |
| `NOT_ALLOWED` | Value is not in the allowed set |
| `DATE_ORDER` | Dates are in an impossible order |
| `DUPLICATE_KEY` | Natural key already seen in this run |

Codes rather than free-text messages, because a data steward should be able to
triage by category and fix a hundred rows at once rather than reading them one
at a time.

### The load is idempotent

The target upserts on the natural key. Running the pipeline twice against the
same source produces the same target state, which matters because you will run
it twice. There is a test asserting exactly this.

### Reconciliation is an output, not an afterthought

`output/reconciliation.md` answers the question the business actually asks,
which is not "did the job succeed" but "did everything arrive":

- rows read, accepted, quarantined, and present in the target
- a balance check that accepted + quarantined equals the source count
- a control total on the money column
- rejections grouped by reason code
- loaded rows grouped by status

## Project layout

```
etl/
  pipeline.py       orchestration: extract, load, quarantine, reconcile
  transforms.py     field-level cleaning functions
  validate.py       type coercion, field rules, cross-field rules, reason codes
config/
  members_mapping.json
data/raw/
  members_legacy.csv    synthetic source with deliberate defects
tests/
  test_pipeline.py      17 tests, no dependencies
output/
  warehouse.db          SQLite target
  reconciliation.md     generated report
```

## Design decisions and trade-offs

**SQLite as the target.** The point of this project is the pipeline, not the
warehouse. SQLite means anyone can clone and run it in one command with nothing
installed. In production this would be Postgres or a cloud warehouse, and the
load step is the only module that would change.

**Streaming extract.** Rows are yielded one at a time rather than read into a
list. It costs nothing here at twenty rows and it is the difference between
working and not working at ten million.

**Errors collected, not raised, per row.** A row with three problems reports all
three rather than only the first one found. Fixing defects one per run is how a
one-week migration becomes a one-month migration.

**Stdlib only.** Deliberate. It keeps the repo runnable years from now and keeps
the logic visible instead of hidden behind a framework. The cost is that heavy
transformations would be slower than a vectorized library, which is the right
trade for a validation-bound workload and the wrong one for a compute-bound one.

## What I would change at scale

- **Chunked loads with a checkpoint.** Ten million rows should commit in batches
  and record progress, so a failure at row nine million resumes rather than
  restarts.
- **Schema drift detection.** Right now an unexpected new column in the source
  is silently ignored. It should fail the run loudly, because a source system
  that changed shape is news.
- **Separate the quarantine lifecycle.** Rejected rows currently live in one
  table and stay there. They need a state machine: new, triaged, corrected,
  reprocessed, so a steward's work is not lost on the next run.
- **Reference data as data.** Plan codes and status values are enumerated in the
  mapping file. Beyond a handful of feeds these belong in a reference table with
  effective dates, because allowed values change and old rows still have to
  validate against the rules that applied when they were written.
- **Orchestration.** A real deployment needs scheduling, retries, alerting, and
  lineage. That is an Airflow or Dagster concern rather than something to
  hand-roll here.

## Licence

MIT. The synthetic data is generated and represents no real person.
