from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass(frozen=True)
class AirtableConfig:
    token: str
    base_id: str


class AirtableError(RuntimeError):
    pass


class AirtableClient:
    def __init__(self, cfg: AirtableConfig, *, timeout_s: float = 30.0) -> None:
        self._cfg = cfg
        self._base_url = f"https://api.airtable.com/v0/{cfg.base_id}"
        self._headers = {
            "Authorization": f"Bearer {cfg.token}",
            "Content-Type": "application/json",
        }
        self._timeout = timeout_s

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=20))
    def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json: Any | None = None) -> Any:
        url = f"{self._base_url}/{path.lstrip('/')}"
        with httpx.Client(timeout=self._timeout) as client:
            r = client.request(method, url, headers=self._headers, params=params, json=json)

        if r.status_code >= 400:
            raise AirtableError(f"{method} {url} -> {r.status_code}: {r.text}")

        return r.json()

    def list_records(
        self,
        table: str,
        *,
        view: str | None = None,
        formula: str | None = None,
        page_size: int = 100,
        max_records: int | None = None,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        offset: str | None = None

        while True:
            params: dict[str, Any] = {"pageSize": page_size}
            if view:
                params["view"] = view
            if formula:
                params["filterByFormula"] = formula
            if offset:
                params["offset"] = offset
            if max_records:
                params["maxRecords"] = max_records

            data = self._request("GET", table, params=params)
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break

        return records

    def create_record(self, table: str, fields: dict[str, Any]) -> dict[str, Any]:
        payload = {"fields": fields}
        return self._request("POST", table, json=payload)

    def update_record(self, table: str, record_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        payload = {"fields": fields}
        return self._request("PATCH", f"{table}/{record_id}", json=payload)

    def upsert_by_formula(
        self,
        table: str,
        *,
        match_formula: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        # Find first match
        matches = self.list_records(table, formula=match_formula, max_records=1)
        if matches:
            record_id = matches[0]["id"]
            return self.update_record(table, record_id, fields)
        return self.create_record(table, fields)
