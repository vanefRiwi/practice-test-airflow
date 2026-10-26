"""Helpers de trazabilidad: títulos de fase y conteos legibles en el log de Airflow."""

import logging

log = logging.getLogger("datamart")


def titulo(texto):
    """Imprime un separador grande para marcar una fase del pipeline."""
    log.info("=" * 78)
    log.info(texto.upper())
    log.info("=" * 78)


def sub(texto):
    """Imprime un subtítulo dentro de una fase."""
    log.info("--- %s", texto)


def formato_numero(valor):
    """Formatea enteros con separador de miles en estilo español (77.040)."""
    return f"{int(valor):,}".replace(",", ".")
