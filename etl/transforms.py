"""Field-level transforms.

Each transform takes a string and returns a string. They are applied in the
order listed in the mapping config, before any validation runs, so that
validation sees the cleaned value rather than the raw one.
"""

import re

_WHITESPACE = re.compile(r"\s+")
_CURRENCY = re.compile(r"[$,\s]")


def strip(value: str) -> str:
    """Trim surrounding whitespace and collapse internal runs of it."""
    return _WHITESPACE.sub(" ", value).strip()


def upper(value: str) -> str:
    return value.upper()


def lower(value: str) -> str:
    return value.lower()


def title_case(value: str) -> str:
    """Title-case a name without destroying O'Brien or Smith-Jones."""
    if not value:
        return value
    parts = re.split(r"([ \-'])", value.lower())
    return "".join(p.capitalize() if p.isalpha() else p for p in parts)


def strip_currency(value: str) -> str:
    """Remove dollar signs, thousands separators, and stray spaces."""
    return _CURRENCY.sub("", value)


def digits_only(value: str) -> str:
    return re.sub(r"\D", "", value)


REGISTRY = {
    "strip": strip,
    "upper": upper,
    "lower": lower,
    "title_case": title_case,
    "strip_currency": strip_currency,
    "digits_only": digits_only,
}


def apply_all(value: str, names) -> str:
    """Apply a list of named transforms in order.

    Raises KeyError on an unknown name rather than silently skipping it, so a
    typo in the config fails loudly at run time instead of quietly passing
    dirty values through to the target.
    """
    for name in names or []:
        if name not in REGISTRY:
            raise KeyError(f"Unknown transform '{name}' in mapping config")
        value = REGISTRY[name](value)
    return value
