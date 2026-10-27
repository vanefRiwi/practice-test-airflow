"""Logging helpers."""

import logging

log = logging.getLogger("datamart")


def section(text):
    """Phase separator in the log."""
    log.info("=" * 78)
    log.info(text.upper())
    log.info("=" * 78)


def step(text):
    """Sub-heading in the log."""
    log.info("--- %s", text)


def fmt_int(value):
    """77040 -> '77,040'."""
    return f"{int(value):,}"
