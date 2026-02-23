from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from source_sage_intacct.client import IntacctClient, IntacctError, format_query_dt


def rfc3339_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_rfc3339(value: str) -> datetime:
    fixed = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(fixed)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_rfc3339(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class SageIntacctBaseStream(Stream):
    primary_key = "RECORDNO"
    cursor_key = "WHENMODIFIED"
    state_checkpoint_interval = 1000

    def __init__(
        self,
        client: IntacctClient,
        config: Dict[str, Any],
        object_name: str,
        stream_name: str,
        supports_incremental: bool = True,
        extra_filter: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.client = client
        self.config = config
        self.object_name = object_name
        self._name = stream_name
        self._supports_incremental = supports_incremental
        self._cursor_field = self.cursor_key if supports_incremental else []
        self.extra_filter = extra_filter
        self._state: Dict[str, Any] = {}
        self._entities_cache: Optional[List[Optional[str]]] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def supports_incremental_sync(self) -> bool:
        return self._supports_incremental

    @property
    def cursor_field(self):
        return self._cursor_field

    @property
    def state(self) -> Mapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: Mapping[str, Any]) -> None:
        self._state = dict(value or {})

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "type": "object",
            "properties": {
                "entity_id": {"type": ["string", "null"]},
                self.primary_key: {"type": ["string", "integer", "null"]},
                self.cursor_key: {"type": ["string", "null"]},
            },
            "additionalProperties": True,
        }

    def entities(self) -> List[Optional[str]]:
        if self._entities_cache is not None:
            return self._entities_cache
        mode = self.config.get("entities_mode", "all")
        if mode == "selected":
            values = self.config.get("entity_ids") or []
            self._entities_cache = [value for value in values if value]
            return self._entities_cache
        discovered = self.client.read_entity_details()
        self._entities_cache = discovered or [None]
        return self._entities_cache

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterator[Mapping[str, Any]]:
        stream_state = dict(stream_state or {})
        state_entities = dict(stream_state.get("entities") or {})
        if not self._supports_incremental:
            for entity_id in self.entities():
                for record in self._read_full_for_entity(entity_id):
                    yield record
            return

        now = rfc3339_now()
        for entity_id in self.entities():
            entity_key = entity_id or "_root"
            current_entity_state = state_entities.get(entity_key) or {}
            cursor_value = current_entity_state.get("cursor")
            start = self._compute_start_time(cursor_value)
            for time_slice in self._generate_slices(start, now):
                max_seen = cursor_value
                for record in self._read_incremental_slice(entity_id, time_slice["start"], time_slice["end"]):
                    wm = record.get(self.cursor_key)
                    if isinstance(wm, str) and wm:
                        if not max_seen or parse_rfc3339(wm) > parse_rfc3339(max_seen):
                            max_seen = wm
                    yield record
                if max_seen:
                    state_entities[entity_key] = {"cursor": max_seen}
                    self.state = {"entities": state_entities}

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_state = dict(current_stream_state or {})
        entities_state = dict(current_state.get("entities") or {})
        entity_id = latest_record.get("entity_id") or "_root"
        latest_cursor = latest_record.get(self.cursor_key)
        if not latest_cursor:
            return current_state
        existing = (entities_state.get(entity_id) or {}).get("cursor")
        if not existing or parse_rfc3339(latest_cursor) > parse_rfc3339(existing):
            entities_state[entity_id] = {"cursor": latest_cursor}
        return {"entities": entities_state}

    def _compute_start_time(self, cursor_value: Optional[str]) -> datetime:
        default_start = parse_rfc3339(self.config["start_date"])
        if not cursor_value:
            return default_start
        lookback_days = int(self.config.get("lookback_days", 3))
        start = parse_rfc3339(cursor_value) - timedelta(days=lookback_days)
        return max(start, default_start)

    def _generate_slices(self, start: datetime, end: datetime) -> Iterator[Dict[str, datetime]]:
        step = timedelta(days=int(self.config.get("slice_step_days", 7)))
        cursor = start
        while cursor < end:
            slice_end = min(cursor + step, end)
            yield {"start": cursor, "end": slice_end}
            cursor = slice_end

    def _read_full_for_entity(self, entity_id: Optional[str]) -> Iterator[Mapping[str, Any]]:
        query = self._build_query(None, None)
        page_size = int(self.config.get("page_size", 1000))
        result = self.client.read_by_query(self.object_name, fields=["*"], query=query, page_size=page_size, entity_id=entity_id)
        for record in result.records:
            record["entity_id"] = entity_id
            yield record
        while result.result_id and result.num_remaining > 0:
            result = self.client.read_more(result.result_id, entity_id=entity_id)
            for record in result.records:
                record["entity_id"] = entity_id
                yield record

    def _read_incremental_slice(self, entity_id: Optional[str], start: datetime, end: datetime) -> Iterator[Mapping[str, Any]]:
        page_size = int(self.config.get("page_size", 1000))
        query = self._build_query(start, end)
        result = self.client.read_by_query(self.object_name, fields=["*"], query=query, page_size=page_size, entity_id=entity_id)
        for record in result.records:
            record["entity_id"] = entity_id
            yield record
        while result.result_id and result.num_remaining > 0:
            result = self.client.read_more(result.result_id, entity_id=entity_id)
            for record in result.records:
                record["entity_id"] = entity_id
                yield record

    def _build_query(self, start: Optional[datetime], end: Optional[datetime]) -> str:
        clauses: List[str] = []
        if start is not None and end is not None:
            clauses.append(f"WHENMODIFIED >= '{format_query_dt(start)}'")
            clauses.append(f"WHENMODIFIED < '{format_query_dt(end)}'")
        if self.extra_filter:
            clauses.append(self.extra_filter)
        if not clauses:
            return "RECORDNO > 0"
        return " AND ".join(clauses)


class ArInvoiceItemsStream(SageIntacctBaseStream):
    def __init__(self, client: IntacctClient, config: Dict[str, Any]) -> None:
        super().__init__(client, config, object_name="ARINVOICEITEM", stream_name="ar_invoice_items", supports_incremental=True)

    def _read_incremental_slice(self, entity_id: Optional[str], start: datetime, end: datetime) -> Iterator[Mapping[str, Any]]:
        try:
            yield from super()._read_incremental_slice(entity_id, start, end)
            return
        except IntacctError as error:
            message = str(error).lower()
            if "whenmodified" not in message and "field" not in message:
                raise

        invoice_query = f"WHENMODIFIED >= '{format_query_dt(start)}' AND WHENMODIFIED < '{format_query_dt(end)}'"
        page_size = int(self.config.get("page_size", 1000))
        result = self.client.read_by_query(
            "ARINVOICE",
            fields=["RECORDNO", "WHENMODIFIED"],
            query=invoice_query,
            page_size=page_size,
            entity_id=entity_id,
        )
        for record in self._iter_items_for_invoice_records(result.records, entity_id):
            yield record
        while result.result_id and result.num_remaining > 0:
            result = self.client.read_more(result.result_id, entity_id=entity_id)
            for record in self._iter_items_for_invoice_records(result.records, entity_id):
                yield record

    def _iter_items_for_invoice_records(self, invoices: Iterable[Dict[str, Any]], entity_id: Optional[str]) -> Iterator[Mapping[str, Any]]:
        for invoice in invoices:
            record_no = invoice.get("RECORDNO")
            if not record_no:
                continue
            full_records = self.client.read("ARINVOICE", keys=[str(record_no)], fields=["*"], entity_id=entity_id)
            for full in full_records:
                items = self._extract_invoice_items(full)
                for item in items:
                    item["INVOICE_RECORDNO"] = record_no
                    item["WHENMODIFIED"] = invoice.get("WHENMODIFIED")
                    item["entity_id"] = entity_id
                    yield item

    def _extract_invoice_items(self, record: Mapping[str, Any]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for key, value in record.items():
            if "ARINVOICEITEM" not in key:
                continue
            if isinstance(value, dict):
                items.append(dict(value))
            elif isinstance(value, list):
                for sub in value:
                    if isinstance(sub, dict):
                        items.append(dict(sub))
        return items


def build_streams(client: IntacctClient, config: Dict[str, Any]) -> List[SageIntacctBaseStream]:
    invoice_docparid = config.get("oe_invoice_docparid", "Sales Invoice").replace("'", "''")
    order_docparid = config.get("oe_order_docparid", "Sales Order").replace("'", "''")
    return [
        SageIntacctBaseStream(client, config, "GLJOURNAL", "gl_journals", supports_incremental=False),
        SageIntacctBaseStream(client, config, "GLBATCH", "gl_batches", supports_incremental=True),
        SageIntacctBaseStream(client, config, "GLENTRY", "gl_entries", supports_incremental=True),
        SageIntacctBaseStream(client, config, "GLDETAIL", "gl_detail", supports_incremental=True),
        SageIntacctBaseStream(client, config, "CUSTOMER", "customers", supports_incremental=True),
        SageIntacctBaseStream(client, config, "ARINVOICE", "ar_invoices", supports_incremental=True),
        ArInvoiceItemsStream(client, config),
        SageIntacctBaseStream(
            client,
            config,
            "SODOCUMENT",
            "oe_invoices",
            supports_incremental=True,
            extra_filter=f"DOCPARID = '{invoice_docparid}'",
        ),
        SageIntacctBaseStream(
            client,
            config,
            "SODOCUMENTENTRY",
            "oe_invoice_lines",
            supports_incremental=True,
            extra_filter=f"DOCPARID = '{invoice_docparid}'",
        ),
        SageIntacctBaseStream(
            client,
            config,
            "SODOCUMENT",
            "orders",
            supports_incremental=True,
            extra_filter=f"DOCPARID = '{order_docparid}'",
        ),
        SageIntacctBaseStream(
            client,
            config,
            "SODOCUMENTENTRY",
            "order_lines",
            supports_incremental=True,
            extra_filter=f"DOCPARID = '{order_docparid}'",
        ),
        SageIntacctBaseStream(
            client,
            config,
            "SODOCUMENTSUBTOTALS",
            "subtotals",
            supports_incremental=True,
            extra_filter=f"DOCPARID = '{invoice_docparid}'",
        ),
        SageIntacctBaseStream(
            client,
            config,
            "SODOCUMENTSUBTOTALS",
            "so_subtotals",
            supports_incremental=True,
            extra_filter=f"DOCPARID = '{order_docparid}'",
        ),
    ]
