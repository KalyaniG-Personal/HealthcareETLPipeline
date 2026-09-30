"""Validation and type coercion.

Every rejection carries a machine-readable reason code. The codes matter more
than they look: they are what turns a quarantine table into something a data
steward can triage by category instead of reading row by row.
"""

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

REASON_CODES = {
    "REQUIRED_MISSING": "Required field was empty",
    "PATTERN_MISMATCH": "Value did not match the expected format",
    "BAD_DATE": "Value could not be parsed as a date",
    "FUTURE_DATE": "Date is in the future",
    "BAD_DECIMAL": "Value could not be parsed as a number",
    "OUT_OF_RANGE": "Numeric value fell outside the allowed range",
    "NOT_ALLOWED": "Value is not in the allowed set",
    "DATE_ORDER": "Dates are in an impossible order",
    "DUPLICATE_KEY": "Natural key already seen in this run",
}


class FieldError(Exception):
    def __init__(self, code: str, field: str, value: str):
        self.code = code
        self.field = field
        self.value = value
        super().__init__(f"{code} on {field}: {value!r}")


def coerce_date(value: str, formats, field: str):
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise FieldError("BAD_DATE", field, value)


def coerce_decimal(value: str, field: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        raise FieldError("BAD_DECIMAL", field, value)


def validate_field(raw_value: str, spec: dict, today: date = None):
    """Validate and coerce one field. Returns the typed value or None.

    An empty optional field returns None rather than raising, which is the
    distinction between "we do not know this" and "this row is broken".
    """
    today = today or date.today()
    field = spec["target"]
    value = raw_value

    if value == "":
        if spec.get("required"):
            raise FieldError("REQUIRED_MISSING", field, value)
        return None

    if "value_map" in spec:
        value = spec["value_map"].get(value, value)

    if spec.get("pattern") and not re.match(spec["pattern"], value):
        raise FieldError("PATTERN_MISMATCH", field, value)

    ftype = spec.get("type", "string")

    if ftype == "date":
        parsed = coerce_date(value, spec.get("input_formats", ["%Y-%m-%d"]), field)
        if spec.get("not_future") and parsed > today:
            raise FieldError("FUTURE_DATE", field, value)
        return parsed

    if ftype == "decimal":
        parsed = coerce_decimal(value, field)
        if "min" in spec and parsed < Decimal(str(spec["min"])):
            raise FieldError("OUT_OF_RANGE", field, value)
        if "max" in spec and parsed > Decimal(str(spec["max"])):
            raise FieldError("OUT_OF_RANGE", field, value)
        return parsed

    if spec.get("allowed_values") and value not in spec["allowed_values"]:
        raise FieldError("NOT_ALLOWED", field, value)

    return value


def validate_record_rules(record: dict, rules) -> None:
    """Cross-field rules that only make sense once every field is typed."""
    for rule in rules or []:
        if rule["type"] == "date_order":
            earlier = record.get(rule["earlier"])
            later = record.get(rule["later"])
            if earlier and later and later < earlier:
                raise FieldError("DATE_ORDER", rule["name"], f"{later} < {earlier}")
