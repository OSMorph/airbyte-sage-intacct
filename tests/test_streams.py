from datetime import datetime, timezone

from source_sage_intacct.streams import SageIntacctBaseStream, parse_rfc3339


class DummyClient:
    def __init__(self):
        self.queries = []

    def read_entity_details(self):
        return ["E1", "E2"]

    def read_by_query(self, object_name, fields, query, page_size, entity_id=None):
        self.queries.append((object_name, query, entity_id))
        return type("R", (), {"records": [], "result_id": None, "num_remaining": 0})()

    def read_more(self, *_args, **_kwargs):
        return type("R", (), {"records": [], "result_id": None, "num_remaining": 0})()


def _config():
    return {
        "start_date": "2024-01-01T00:00:00Z",
        "lookback_days": 3,
        "slice_step_days": 7,
        "page_size": 1000,
        "entities_mode": "all",
        "entity_ids": [],
        "oe_invoice_docparid": "Sales Invoice",
        "oe_order_docparid": "Sales Order",
    }


def test_slice_generation_respects_boundaries():
    stream = SageIntacctBaseStream(DummyClient(), _config(), "GLBATCH", "gl_batches", True)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 20, tzinfo=timezone.utc)
    slices = list(stream._generate_slices(start, end))
    assert len(slices) == 3
    assert slices[0]["start"] == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert slices[-1]["end"] == end


def test_state_updates_per_entity():
    stream = SageIntacctBaseStream(DummyClient(), _config(), "GLBATCH", "gl_batches", True)
    state = {}
    state = stream.get_updated_state(state, {"entity_id": "E1", "WHENMODIFIED": "2024-01-02T00:00:00Z"})
    state = stream.get_updated_state(state, {"entity_id": "E2", "WHENMODIFIED": "2024-01-03T00:00:00Z"})
    assert state["entities"]["E1"]["cursor"] == "2024-01-02T00:00:00Z"
    assert state["entities"]["E2"]["cursor"] == "2024-01-03T00:00:00Z"


def test_order_entry_query_contains_docparid_filter():
    client = DummyClient()
    stream = SageIntacctBaseStream(
        client,
        _config(),
        "SODOCUMENT",
        "orders",
        True,
        extra_filter="DOCPARID = 'Sales Order'",
    )
    list(stream._read_incremental_slice("E1", datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 2, tzinfo=timezone.utc)))
    assert "DOCPARID = 'Sales Order'" in client.queries[0][1]


def test_parse_intacct_datetime_format():
    parsed = parse_rfc3339("01/26/2026 13:37:29")
    assert parsed == datetime(2026, 1, 26, 13, 37, 29, tzinfo=timezone.utc)
