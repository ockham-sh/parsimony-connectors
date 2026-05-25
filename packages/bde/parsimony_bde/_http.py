"""BdE REST and catalog CSV transport."""

from __future__ import annotations

BASE_URL = "https://app.bde.es/bierest/resources/srdatosapp"
CATALOG_CSV_BASE_URL = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv"

CATALOG_CHAPTERS: tuple[tuple[str, str], ...] = (
    ("be", "General Statistics"),
    ("cf", "Financial Accounts"),
    ("ie", "International Economy"),
    ("pb", "Bank Lending Survey"),
    ("si", "Financial Indicators"),
    ("tc", "Exchange Rates"),
    ("ti", "Interest Rates"),
)

CSV_ENCODING = "cp1252"
CSV_HEADERS: tuple[str, ...] = (
    "serie",
    "seq",
    "alias",
    "file",
    "description",
    "var_type",
    "unit_code",
    "exponent",
    "decimals",
    "unit_desc",
    "frequency_raw",
    "start_date",
    "end_date",
    "n_obs",
    "title",
    "source_org",
    "notes",
)

FREQ_MAP_RAW = {
    "DIARIA": "Daily",
    "LABORABLE": "Business Daily",
    "SEMANAL": "Weekly",
    "QUINCENAL": "Bi-weekly",
    "MENSUAL": "Monthly",
    "TRIMESTRAL": "Quarterly",
    "SEMESTRAL": "Semi-annual",
    "ANUAL": "Annual",
}

FREQ_MAP = {
    "D": "Daily",
    "M": "Monthly",
    "Q": "Quarterly",
    "A": "Annual",
    "S": "Semi-annual",
    "W": "Weekly",
    "B": "Business Daily",
}

COLUMN_MAP = {
    "serie": "key",
    "descripcion": "description",
    "descripcionCorta": "title",
    "codFrecuencia": "freq",
    "decimales": "decimals",
    "simbolo": "symbol",
    "fechaInicio": "start_date",
    "fechaFin": "end_date",
    "fechas": "date",
    "valores": "value",
}
