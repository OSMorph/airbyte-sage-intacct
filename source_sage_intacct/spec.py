from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict


DEFAULT_API_URL = "https://api.intacct.com/ia/xml/xmlgw.phtml"


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(config)
    normalized.setdefault("api_url", DEFAULT_API_URL)
    normalized.setdefault("lookback_days", 3)
    normalized.setdefault("page_size", 1000)
    normalized.setdefault("slice_step_days", 7)
    normalized.setdefault("schema_sample_size", 200)
    normalized.setdefault("entities_mode", "all")
    normalized.setdefault("entity_ids", [])
    normalized.setdefault("oe_invoice_docparid", "Sales Invoice")
    normalized.setdefault("oe_order_docparid", "Sales Order")
    if "start_date" not in normalized:
        start = datetime.now(timezone.utc) - timedelta(days=30)
        normalized["start_date"] = start.isoformat().replace("+00:00", "Z")
    return normalized
