"""BdE REST and catalog CSV transport."""

from __future__ import annotations

BASE_URL = "https://app.bde.es/bierest/resources/srdatosapp"
CATALOG_CSV_BASE_URL = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv"
CATALOG_ZIP_BASE_URL = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/zip"

# Six of the seven published catalog chapters list API-fetchable ``serie`` codes
# directly in their CSV. The seventh — ``pb`` (Bank Lending Survey) — is the
# odd one out: its CSV ``serie`` column holds *family/table* codes (``PB_1_1.1``)
# that the BIEST web service rejects with HTTP 412 "no existe". The real
# fetchable codes for that survey live only inside the bulk ``pb.zip`` value
# files, so ``pb`` is enumerated from the ZIP instead (see ``PB_ZIP_URL`` and
# ``connectors/_catalog.parse_pb_zip``). Live-verified 2026-06-08.
CATALOG_CHAPTERS: tuple[tuple[str, str], ...] = (
    ("be", "General Statistics"),
    ("cf", "Financial Accounts"),
    ("ie", "International Economy"),
    ("si", "Financial Indicators"),
    ("tc", "Exchange Rates"),
    ("ti", "Interest Rates"),
)

# Bank Lending Survey — recovered from the bulk ZIP, not the CSV catalog.
PB_ZIP_URL = f"{CATALOG_ZIP_BASE_URL}/pb.zip"
PB_CATEGORY = "Bank Lending Survey"

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
