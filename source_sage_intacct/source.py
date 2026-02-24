from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set, Tuple

from airbyte_cdk.models import AirbyteStateMessage, ConfiguredAirbyteCatalog, ConnectorSpecification
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from source_sage_intacct.client import IntacctAuthError, IntacctClient, IntacctError, IntacctPermissionError
from source_sage_intacct.spec import normalize_config
from source_sage_intacct.streams import SageIntacctBaseStream, build_streams


class SourceSageIntacct(AbstractSource):
    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        spec_path = Path(__file__).resolve().parents[1] / "spec.json"
        with spec_path.open("r", encoding="utf-8") as handle:
            spec = json.load(handle)
        return ConnectorSpecification(**spec)

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        normalized = normalize_config(dict(config))
        client = IntacctClient(normalized)
        try:
            entities = self._resolve_entities(client, normalized)
            if normalized.get("entities_mode") == "all" and not entities:
                return False, "No accessible entities returned by readEntityDetails."

            probe_entity = entities[0] if entities else None
            probe_error: Optional[Exception] = None
            for probe_object in ("CUSTOMER", "ARINVOICE", "SODOCUMENT"):
                try:
                    client.read_by_query(probe_object, ["RECORDNO"], "RECORDNO > 0", 1, entity_id=probe_entity)
                    probe_error = None
                    break
                except IntacctPermissionError as error:
                    probe_error = error
            if probe_error:
                raise probe_error

            for object_name in ("SODOCUMENT", "SODOCUMENTENTRY", "SODOCUMENTSUBTOTALS"):
                client.lookup(object_name, entity_id=probe_entity)
            return True, None
        except (IntacctAuthError, IntacctPermissionError, IntacctError) as error:
            return False, str(error)
        finally:
            client.close()

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        normalized = normalize_config(dict(config))
        client = IntacctClient(normalized)
        streams = build_streams(client, normalized)
        for stream in streams:
            stream.get_json_schema = self._schema_getter(client, stream, normalized)  # type: ignore[method-assign]
        return streams

    def read(
        self,
        logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ):
        stream_state = self._state_to_mapping(state)
        yield from super().read(logger=logger, config=config, catalog=catalog, state=stream_state)

    def _schema_getter(self, client: IntacctClient, stream: SageIntacctBaseStream, config: Dict[str, Any]):
        def _get_schema() -> Mapping[str, Any]:
            try:
                entities = self._resolve_entities(client, config)
                entity_id = entities[0] if entities else None
                inferred = stream.infer_json_schema(entity_id)
                lookup = client.lookup(stream.object_name, entity_id=entity_id)
                self._merge_lookup_fields(inferred, lookup)
                return inferred
            except Exception:
                return stream.get_json_schema()

        return _get_schema

    @classmethod
    def _to_json_schema_type(cls, value: Any) -> Dict[str, Any]:
        lowered = str(value or "").lower()
        if "int" in lowered or "number" in lowered or "decimal" in lowered:
            return {"type": ["number", "null"]}
        if "date" in lowered or "time" in lowered:
            return {"type": ["string", "null"]}
        if "bool" in lowered:
            return {"type": ["boolean", "null"]}
        return {"type": ["string", "null"]}

    @classmethod
    def _merge_lookup_fields(cls, schema: Mapping[str, Any], lookup: Mapping[str, Any]) -> None:
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            return

        for field_name, datatype in cls._extract_lookup_fields(lookup):
            if field_name in properties:
                continue
            properties[field_name] = cls._to_json_schema_type(datatype)

    @classmethod
    def _extract_lookup_fields(cls, value: Any) -> Set[Tuple[str, str]]:
        fields: Set[Tuple[str, str]] = set()

        def _walk(node: Any) -> None:
            if isinstance(node, dict):
                name = node.get("NAME")
                if isinstance(name, str) and name:
                    dtype = str(node.get("DATATYPE") or node.get("TYPE") or "")
                    fields.add((name, dtype))
                for child in node.values():
                    _walk(child)
                return
            if isinstance(node, list):
                for child in node:
                    _walk(child)

        _walk(value)
        return fields

    @staticmethod
    def _resolve_entities(client: IntacctClient, config: Mapping[str, Any]) -> List[Optional[str]]:
        if config.get("entities_mode") == "selected":
            selected = [entity for entity in config.get("entity_ids", []) if entity]
            return selected or [None]
        discovered = client.read_entity_details()
        return discovered or [None]

    @staticmethod
    def _state_to_mapping(state: Optional[List[AirbyteStateMessage]]) -> MutableMapping[str, Any]:
        if not state:
            return {}
        mapping: MutableMapping[str, Any] = {}
        for state_message in state:
            stream_state = state_message.stream
            if not stream_state:
                continue
            stream_name = stream_state.stream_descriptor.name if stream_state.stream_descriptor else None
            data = stream_state.stream_state
            if stream_name and isinstance(data, dict):
                mapping[stream_name] = data
        return mapping
