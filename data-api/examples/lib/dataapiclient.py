from __future__ import annotations
from collections.abc import Callable
import requests


class DsbDataApiClient:
    def __init__(self, base_url: str, token_provider: Callable[[], str]):
        self.base_url = base_url
        self.token_provider = token_provider

        req = (
            DsbDataApiRequest(base_url, token_provider)
            .select("asd", "asdasd", "asdasd")
            .order_by("asd ASC", "asd DESC")
            .filter(
                ("asd", "eq", "asdasd"),
                ("asd", "gt", "asdasd")
            )
            .collect()
        )


    def get_dataset(self, dataset_name: str) -> requests.Response:
        url = f"{self.base_url}/datasets/{dataset_name}"
        headers = {
            "Authorization": f"Bearer {self.token_provider()}"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response
    
class DsbDataApiRequest:
    def __init__(self, base_url: str, token_provider: callable):
        self.base_url = base_url
        self.token_provider = token_provider
        self._format: str = "parquet"
        self._select: list[str] = []
        self._order_by: list[str] = []
        self._filters: list[tuple[str, str, str]] = []

    def format(self, value: str) -> str:
        """Format a value for use in a query."""
        self._format = value
        return str(value)

    def select(self, *fields: str) -> DsbDataApiRequest:
        """Specify which columns to select from the dataset."""
        self._select.extend(fields)
        return self

    def order_by(self, *fields: str) -> DsbDataApiRequest:
        """Specify the order in which to return the results."""
        self._order_by.extend(fields)
        return self

    def filter(self, *filters: tuple[str, str, str]) -> DsbDataApiRequest:
        """Filter the results based on the specified criteria."""
        self._filters.extend(filters)
        return self

    def collect(self) -> requests.Response:
        url = f"{self.base_url}/datasets"
        params = {}
        if self._select:
            params["select"] = ",".join(self._select)
        if self._order_by:
            params["order_by"] = ",".join(self._order_by)
        if self._filters:
            filter_expressions = [f"{field} {op} '{value}'" for field, op, value in self._filters]
            params["filter"] = " AND ".join(filter_expressions)
        if self._select:
            select_param = ",".join(self._select)
            url = f"{url}?select={select_param}"
        headers = {
            "Authorization": f"Bearer {self.token_provider()}"
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response
