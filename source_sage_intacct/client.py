from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

import requests


class IntacctError(Exception):
    pass


class IntacctAuthError(IntacctError):
    pass


class IntacctPermissionError(IntacctError):
    pass


class IntacctTransientError(IntacctError):
    pass


def element_to_value(node: ET.Element) -> Any:
    children = list(node)
    if not children:
        return (node.text or "").strip()
    grouped: Dict[str, List[Any]] = {}
    for child in children:
        grouped.setdefault(child.tag, []).append(element_to_value(child))
    result: Dict[str, Any] = {}
    for key, values in grouped.items():
        result[key] = values[0] if len(values) == 1 else values
    return result


def coerce_iso(value: str) -> str:
    if value.endswith("Z"):
        return value
    return f"{value}Z"


def parse_iso(value: str) -> datetime:
    fixed = value.replace("Z", "+00:00")
    return datetime.fromisoformat(fixed)


def format_query_dt(value: datetime) -> str:
    return value.strftime("%m/%d/%Y %H:%M:%S")


@dataclass
class QueryResult:
    records: List[Dict[str, Any]]
    result_id: Optional[str]
    num_remaining: int


class IntacctClient:
    def __init__(self, config: Dict[str, Any], timeout_seconds: int = 60) -> None:
        self.config = config
        self.api_url = config["api_url"]
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def close(self) -> None:
        self.session.close()

    def read_entity_details(self) -> List[str]:
        root = self._call_function("readEntityDetails", ET.Element("readEntityDetails"))
        records = self._parse_records(root)
        entity_ids: List[str] = []
        for record in records:
            for key in ("ENTITYID", "LOCATIONID", "ID"):
                raw = record.get(key)
                if isinstance(raw, str) and raw:
                    entity_ids.append(raw)
                    break
        return sorted(set(entity_ids))

    def lookup(self, object_name: str, entity_id: Optional[str] = None) -> Dict[str, Any]:
        fn = ET.Element("lookup")
        object_node = ET.SubElement(fn, "object")
        object_node.text = object_name
        root = self._call_function("lookup", fn, entity_id=entity_id)
        records = self._parse_records(root)
        return records[0] if records else {}

    def read_by_query(
        self,
        object_name: str,
        fields: Iterable[str],
        query: str,
        page_size: int,
        entity_id: Optional[str] = None,
    ) -> QueryResult:
        fn = ET.Element("readByQuery")
        object_node = ET.SubElement(fn, "object")
        object_node.text = object_name
        fields_node = ET.SubElement(fn, "fields")
        fields_node.text = ",".join(fields)
        query_node = ET.SubElement(fn, "query")
        query_node.text = query
        pagesize_node = ET.SubElement(fn, "pagesize")
        pagesize_node.text = str(page_size)
        root = self._call_function("readByQuery", fn, entity_id=entity_id)
        records = self._parse_records(root)
        return QueryResult(
            records=records,
            result_id=self._text(root, ".//resultId"),
            num_remaining=int(self._text(root, ".//numremaining") or "0"),
        )

    def read_more(self, result_id: str, entity_id: Optional[str] = None) -> QueryResult:
        fn = ET.Element("readMore")
        result_node = ET.SubElement(fn, "resultId")
        result_node.text = result_id
        root = self._call_function("readMore", fn, entity_id=entity_id)
        records = self._parse_records(root)
        return QueryResult(
            records=records,
            result_id=self._text(root, ".//resultId") or result_id,
            num_remaining=int(self._text(root, ".//numremaining") or "0"),
        )

    def read(self, object_name: str, keys: List[str], fields: Iterable[str], entity_id: Optional[str] = None) -> List[Dict[str, Any]]:
        fn = ET.Element("read")
        object_node = ET.SubElement(fn, "object")
        object_node.text = object_name
        keys_node = ET.SubElement(fn, "keys")
        keys_node.text = ",".join(keys)
        fields_node = ET.SubElement(fn, "fields")
        fields_node.text = ",".join(fields)
        root = self._call_function("read", fn, entity_id=entity_id)
        return self._parse_records(root)

    def _call_function(self, function_name: str, fn_body: ET.Element, entity_id: Optional[str] = None) -> ET.Element:
        request_xml = self._build_request_xml(fn_body, entity_id)
        backoff = 1.0
        for attempt in range(4):
            try:
                response = self.session.post(
                    self.api_url,
                    data=request_xml.encode("utf-8"),
                    headers={"Content-Type": "application/xml"},
                    timeout=self.timeout_seconds,
                )
                if response.status_code >= 500:
                    raise IntacctTransientError(f"HTTP {response.status_code} for {function_name}")
                if response.status_code >= 400:
                    raise IntacctError(f"HTTP {response.status_code} for {function_name}: {response.text[:500]}")
                root = ET.fromstring(response.text)
                self._raise_if_failure(root)
                return root
            except (requests.Timeout, requests.ConnectionError, IntacctTransientError):
                if attempt == 3:
                    raise
                time.sleep(backoff)
                backoff *= 2

        raise IntacctError(f"Retry budget exhausted for {function_name}")

    def _build_request_xml(self, fn_body: ET.Element, entity_id: Optional[str]) -> str:
        request = ET.Element("request")
        control = ET.SubElement(request, "control")
        self._append_text(control, "senderid", self.config["sender_id"])
        self._append_text(control, "password", self.config["sender_password"])
        self._append_text(control, "controlid", str(uuid.uuid4()))
        self._append_text(control, "uniqueid", "false")
        self._append_text(control, "dtdversion", "3.0")
        self._append_text(control, "includewhitespace", "false")

        operation = ET.SubElement(request, "operation")
        authentication = ET.SubElement(operation, "authentication")
        login = ET.SubElement(authentication, "login")
        self._append_text(login, "userid", self.config["user_id"])
        self._append_text(login, "companyid", self.config["company_id"])
        self._append_text(login, "password", self.config["user_password"])
        if entity_id:
            self._append_text(login, "locationid", entity_id)

        content = ET.SubElement(operation, "content")
        function = ET.SubElement(content, "function")
        function.set("controlid", str(uuid.uuid4()))
        function.append(fn_body)
        return ET.tostring(request, encoding="unicode")

    @staticmethod
    def _append_text(parent: ET.Element, tag: str, value: str) -> None:
        node = ET.SubElement(parent, tag)
        node.text = value

    @staticmethod
    def _text(root: ET.Element, xpath: str) -> Optional[str]:
        node = root.find(xpath)
        if node is None:
            return None
        text = node.text or ""
        return text.strip() or None

    def _raise_if_failure(self, root: ET.Element) -> None:
        control_status = self._text(root, "./control/status")
        if control_status and control_status.lower() != "success":
            message = self._text(root, ".//description2") or self._text(root, ".//description") or "Control failure"
            raise IntacctError(message)

        auth_status = self._text(root, "./operation/authentication/status")
        if auth_status and auth_status.lower() != "success":
            message = self._text(root, "./operation/authentication/errormessage/error/description2") or "Auth failure"
            raise IntacctAuthError(message)

        for result in root.findall("./operation/result"):
            status = self._text(result, "./status")
            if not status or status.lower() == "success":
                continue
            message = self._text(result, "./errormessage/error/description2") or self._text(result, "./errormessage/error/description") or "Operation failure"
            error_no = self._text(result, "./errormessage/error/errorno") or ""
            lowered = message.lower()
            if "permission" in lowered or error_no in {"WSP001"}:
                raise IntacctPermissionError(message)
            if "timeout" in lowered or "temporarily unavailable" in lowered:
                raise IntacctTransientError(message)
            if "login" in lowered or "authentication" in lowered:
                raise IntacctAuthError(message)
            raise IntacctError(message)

    def _parse_records(self, root: ET.Element) -> List[Dict[str, Any]]:
        result_records: List[Dict[str, Any]] = []
        for data_node in root.findall(".//result/data"):
            for child in list(data_node):
                if child.tag in {"error", "warnings"}:
                    continue
                value = element_to_value(child)
                if isinstance(value, dict):
                    result_records.append(value)
                else:
                    result_records.append({child.tag: value})
        return result_records

