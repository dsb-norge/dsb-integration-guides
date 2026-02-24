from __future__ import annotations
import io
import os
import requests
from abc import ABC, abstractmethod
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging


class DsbDataApiClient:
    def __init__(
        self,
        base_url: str | None = None,
        auth_config: dict | None = None,
    ):
        self.base_url = base_url or os.getenv(
            "DSB_DATA_API_BASE_URL", "https://data.dsb.no/api/v1"
        )
        if not self.base_url.endswith("/api/v1"):
            self.base_url = f"{self.base_url}/api/v1"
        self.token_provider = MaskinportenTokenProvider(auth_config)
        # Logging setup
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)

    def get_dataset(self, dataset_name: str, full: bool = True) -> DsbDataApiRequest:
        return DsbDataApiRequest(
            dataset_url=f"{self.base_url}/datasets/{dataset_name}",
            full=full,
            bearer_token=self.token_provider.get_token(),
            logger=self.log,
        )


class DsbDataApiRequest:
    def __init__(
        self,
        dataset_url: str,
        full: bool,
        bearer_token: str,
        logger: logging.Logger | None = None,
    ):
        self.dataset_url = dataset_url
        self.bearer_token = bearer_token
        self.full = full
        self._top: int | None = None
        self._skip: int | None = None
        self._select: list[str] = []
        self._exclude: list[str] = []
        self._order_by: list[tuple[str, str | None]] = []
        self._filters: list[tuple[str, str, str]] = []
        self.log = logger or logging.getLogger(__name__)

    def top(self, size: int) -> DsbDataApiRequest:
        """Set the number of results to return per page."""
        self._top = size
        return self

    def page_size(self, size: int) -> DsbDataApiRequest:
        """Set the number of results to return per page. (Alias for top())"""
        self.top(size)
        return self

    def skip(self, count: int) -> DsbDataApiRequest:
        """Set the number of results to skip."""
        self._skip = count
        return self

    def select(self, *fields: str) -> DsbDataApiRequest:
        """Specify which columns to select from the dataset. Cannot be used together with exclude()."""
        assert not self._exclude, "Cannot use select and exclude together."
        self._select.extend(fields)
        return self

    def exclude(self, *fields: str) -> DsbDataApiRequest:
        """Specify which columns to exclude from the dataset. Cannot be used together with select()."""
        assert not self._select, "Cannot use select and exclude together."
        self._exclude.extend(fields)
        return self

    def order_by(self, *fields: str | tuple[str, str]) -> DsbDataApiRequest:
        """Specify the order in which to return the results.

        Args:
            *fields: Can be either:
                - String: column name (defaults to ascending order)
                - Tuple: (column_name, direction) where direction is 'asc', 'desc', or None

        Examples:
            .order_by("name")  # name ascending
            .order_by("name", "age")  # name asc, age asc
            .order_by(("name", "desc"), "age")  # name desc, age asc
            .order_by(("name", "desc"), ("age", "asc"))  # name desc, age asc
        """
        processed_fields = []

        for field in fields:
            if isinstance(field, str):
                # Simple string field - default use default direction of the api
                processed_fields.append((field, None))
            elif isinstance(field, tuple) and len(field) == 2:
                # Tuple with (field, direction)
                field_name, direction = field
                if direction.lower() not in ["asc", "desc"]:
                    raise ValueError(
                        f"Invalid order direction: {direction}. Must be 'asc', 'desc', or None."
                    )
                processed_fields.append((field_name, direction))
            else:
                raise ValueError(
                    f"Invalid field format: {field}. Must be string or tuple of (field_name, direction)."
                )

        self._order_by.extend(processed_fields)
        return self

    def filter(self, *filters: tuple[str, str, str]) -> DsbDataApiRequest:
        """Filter the results based on the specified criteria."""
        self._filters.extend(filters)
        return self

    def collect(self) -> DsbDataApiResponse:
        # Setup request
        url = f"{self.dataset_url}"
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
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
            params["$orderby"] = ",".join(
                [
                    f"{field} {direction}" if direction else field
                    for field, direction in self._order_by
                ]
            )
        if self._filters:
            filter_expressions = [
                f"{field} {op} '{value}'" for field, op, value in self._filters
            ]
            params["$filter"] = " and ".join(filter_expressions)

        # Fetch all pages
        responses = []
        max_retries = 2
        current_retry = 0
        total_retries = 0
        current_page = 1
        while True:
            if len(responses) == 0:
                self.log.info(f"Fetching page from {url} with params {params}")
            else:
                message = f"Fetching next page from {url} with params {params}"
                if total_retries > 0:
                    message += f" (total retries so far: {total_retries + 1})"
                self.log.info(message)

            response = requests.get(url, headers=headers, params=params)
            responses.append(response)
            if response.status_code != 200:
                self.log.error(
                    f"Request failed with status code {response.status_code}\n: {response.json()}"
                )
                if current_retry < max_retries:
                    current_retry += 1
                    total_retries += 1
                    continue
                raise Exception(
                    f"Request failed with status code {response.status_code}\n: {response.json()}"
                )

            # If not fetching full dataset, break after first page
            if not self.full:
                break

            if "X-Total-Pages" in response.headers:
                total_pages = int(response.headers.get("X-Total-Pages", "1"))
                self.log.info(f"Current page: {current_page} / {total_pages}")
                current_page += 1

            # Pagination strategy #1: Using Link header
            if response.headers.get("Link") is not None:
                links = str(response.headers.get("Link")).split(",")
                next_link = None
                for link in links:
                    parts = link.split(";")
                    if len(parts) > 1 and 'rel="next"' in parts[1]:
                        next_link = parts[0].strip("<> ")
                        break
                if next_link:
                    params["$skip"] = next_link.split("$skip=")[-1].split("&")[
                        0
                    ]  # Keep track of $skip value from next link in-case something goes wrong where the api dont return next link when it should
                    url = next_link
                else:
                    break
            # Pagination strategy #2: Using X-Current-Page and X-Total-Pages headers
            elif response.headers.get("X-Current-Page") != response.headers.get(
                "X-Total-Pages"
            ):
                params["$skip"] = str(
                    int(params.get("$skip", "0")) + int(params.get("$top", "1000"))
                )
            # No more pages
            else:
                break
            current_retry = 0  # Reset retry counter on successful request

        # Combine responses
        parquet_blobs = [r.content for r in responses if r.status_code == 200]
        tables = [pq.read_table(io.BytesIO(b)) for b in parquet_blobs]
        combined_table = pa.concat_tables(tables)

        return DsbDataApiResponse(df=combined_table.to_pandas())


class DsbDataApiResponse:
    def __init__(self, df: pd.DataFrame):
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
        "DSB_DATA_API_AUTH_MASKINPORTEN_CLIENT_PRIVATE_KEY_PATH",
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
            private_key=open(
                self.config.get(
                    "DSB_DATA_API_AUTH_MASKINPORTEN_CLIENT_PRIVATE_KEY_PATH", ""
                ),
                "rb",
            ).read(),
        )
