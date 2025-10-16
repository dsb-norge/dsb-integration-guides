from __future__ import annotations
import io
import os
import requests
from abc import ABC, abstractmethod
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class DsbDataApiClient:
    def __init__(
            self,
            base_url: str | None = None,
            auth_config: dict | None = None,
        ):
        self.base_url = base_url or os.getenv("DSB_DATA_API_BASE_URL", "https://data.dsb.no/api/v1")
        if not self.base_url.endswith("/api/v1"):
            self.base_url = f"{self.base_url}/api/v1"
        self.token_provider = MaskinportenTokenProvider(auth_config)

    def get_dataset(self, dataset_name: str, full: bool = True) -> DsbDataApiRequest:
        return DsbDataApiRequest(
            dataset_url=f"{self.base_url}/datasets/{dataset_name}",
            full=full,
            bearer_token=self.token_provider.get_token()
        )
    
    
class DsbDataApiRequest:
    def __init__(
        self,
        dataset_url: str,
        full: bool,
        bearer_token: str,
    ):
        self.dataset_url = dataset_url
        self.bearer_token = bearer_token
        self.full = full
        self._top: int | None = None
        self._skip: int | None = None
        self._select: list[str] = []
        self._exclude: list[str] = []
        self._order_by: list[str] = []
        self._filters: list[tuple[str, str, str]] = []

    def format(self, value: str) -> str:
        """Format a value for use in a query."""
        if value not in ["parquet", "json", "csv", "xml"]:
            raise ValueError(f"Invalid format: {value}")
        self._format = value
        return str(value)

    def top(self, size: int) -> DsbDataApiRequest:
        """Set the number of results to return per page."""
        self._top = size
        return self

    def skip(self, count: int) -> DsbDataApiRequest:
        """Set the number of results to skip."""
        self._skip = count
        return self

    def select(self, *fields: str) -> DsbDataApiRequest:
        """Specify which columns to select from the dataset."""
        assert not self._exclude, "Cannot use select and exclude together."
        self._select.extend(fields)
        return self

    def exclude(self, *fields: str) -> DsbDataApiRequest:
        """Specify which columns to exclude from the dataset."""
        assert not self._select, "Cannot use select and exclude together."
        self._exclude.extend(fields)
        return self

    def order_by(self, *fields: str) -> DsbDataApiRequest:
        """Specify the order in which to return the results."""
        self._order_by.extend(fields)
        return self

    def filter(self, *filters: tuple[str, str, str]) -> DsbDataApiRequest:
        """Filter the results based on the specified criteria."""
        self._filters.extend(filters)
        return self
    
    def collect(self) -> DsbDataApiResponse:
        # Setup request
        url = f"{self.dataset_url}"
        headers = {
            "Authorization": f"Bearer {self.bearer_token}"
        }
        params = {}
        if self._top:
            params["$top"] = str(self._top)
        if self._skip:
            params["$skip"] = str(self._skip)
        if self._select:
            params["$select"] = ",".join(self._select)
        if self._exclude:
            params["$exclude"] = ",".join(self._exclude)
        if self._order_by:
            params["$order_by"] = ",".join(self._order_by)
        if self._filters:
            filter_expressions = [f"{field} {op} '{value}'" for field, op, value in self._filters]
            params["$filter"] = " and ".join(filter_expressions)

        print("Headers:", headers)
        print(f"Requesting data from {url} with params {params}")

        # Fetch all pages
        responses = []
        max_retries = 2
        current_retry = 0
        total_retries = 0
        while True:
            response = requests.get(url, headers=headers, params=params)
            print(f"Response status code: {response.status_code}")
            responses.append(response)
            if response.status_code != 200:
                if current_retry < max_retries:
                    current_retry += 1
                    total_retries += 1
                    continue
                raise Exception(f"Request failed with status code {response.status_code}\n: { response.json()}")
            if not self.full:
                break

            if response.headers.get("X-Current-Page") != response.headers.get("X-Total-Pages"):
                params["$skip"] = str(int(params.get("$skip", "0")) + int(params.get("$top", "1000")))
            else:
                break
            current_retry = 0  # Reset retry counter on successful request

        # Combine responses
        parquet_blobs = [r.content for r in responses if r.status_code == 200]
        tables = [
            pq.read_table(io.BytesIO(b))
            for b in parquet_blobs
        ]
        combined_table = pa.concat_tables(tables)
        
        return DsbDataApiResponse(
            df=combined_table.to_pandas()
        )

class DsbDataApiResponse:
    def __init__(
        self,
        df: pd.DataFrame
    ):
        self.df = df

    def to_dataframe(self) -> pd.DataFrame:
        return self.df

class DsbDataApiTokenProvider(ABC):
    @abstractmethod
    def __init__(self, config: dict):
        pass

    @abstractmethod
    def get_token(self) -> str:
        """Get a token for accessing the DSB Data API."""
        pass

class MaskinportenTokenProvider(DsbDataApiTokenProvider):
    requires = [
        "DSB_DATA_API_AUTH_MASKINPORTEN_CLIENT_KEY_ID",
        "DSB_DATA_API_AUTH_MASKINPORTEN_CLIENT_ID",
        "DSB_DATA_API_AUTH_MASKINPORTEN_AUDIENCE",
        "DSB_DATA_API_AUTH_MASKINPORTEN_SCOPE",
        "DSB_DATA_API_AUTH_MASKINPORTEN_RESOURCE",
        "DSB_DATA_API_AUTH_MASKINPORTEN_CLIENT_PRIVATE_KEY_PATH"
    ]

    def __init__(self, config: dict | None = None):
        self.config = config or {}
        
        for key in self.requires:
            if self.config.get(key) is None:
                val = os.getenv(key)
                if val is not None:
                    self.config[key] = val

        missing_keys = [key for key in self.requires if key not in self.config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {', '.join(missing_keys)}")

    def get_token(self) -> str:
        from lib.maskinporten import get_access_token
        return get_access_token(
            key_id=self.config.get("DSB_DATA_API_AUTH_MASKINPORTEN_CLIENT_KEY_ID", ""),
            client_id=self.config.get("DSB_DATA_API_AUTH_MASKINPORTEN_CLIENT_ID", ""),
            audience=self.config.get("DSB_DATA_API_AUTH_MASKINPORTEN_AUDIENCE", ""),
            scope=self.config.get("DSB_DATA_API_AUTH_MASKINPORTEN_SCOPE", ""),
            resource=self.config.get("DSB_DATA_API_AUTH_MASKINPORTEN_RESOURCE", ""),
            private_key=open(self.config.get("DSB_DATA_API_AUTH_MASKINPORTEN_CLIENT_PRIVATE_KEY_PATH", ""), "rb").read()
        )
