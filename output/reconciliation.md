# Reconciliation report: member_enrollment

Run date: 2026-09-29
Source file: `data/raw/members_legacy.csv`
Target table: `members`

## Row counts

| Measure | Count |
| --- | ---: |
| Rows read from source | 20 |
| Rows accepted | 10 |
| Rows quarantined | 10 |
| Rows in target table | 10 |

**Balance check:** accepted + quarantined = 20, source = 20 PASS

## Control totals

Sum of monthly_premium in target: 10,546.50

## Rejections by reason code

| Reason code | Count |
| --- | ---: |
| NOT_ALLOWED | 2 |
| REQUIRED_MISSING | 1 |
| PATTERN_MISMATCH | 1 |
| OUT_OF_RANGE | 1 |
| FUTURE_DATE | 1 |
| DUPLICATE_KEY | 1 |
| DATE_ORDER | 1 |
| BAD_DECIMAL | 1 |
| BAD_DATE | 1 |

## Loaded rows by status

| Status | Count |
| --- | ---: |
| ACTIVE | 6 |
| TERMINATED | 3 |
| PENDING | 1 |
