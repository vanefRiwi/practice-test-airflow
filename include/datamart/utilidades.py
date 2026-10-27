"""Helpers para los logs."""

import logging

log = logging.getLogger("datamart")


def titulo(texto):
    """Separador de fase en el log."""
    log.info("=" * 78)
    log.info(texto.upper())
    log.info("=" * 78)


def sub(texto):
    """Subtítulo en el log."""
    log.info("--- %s", texto)


def formato_numero(valor):
    """77040 -> '77.040'."""
    return f"{int(valor):,}".replace(",", ".")
