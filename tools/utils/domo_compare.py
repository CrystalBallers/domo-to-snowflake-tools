import pandas as pd
import requests
import logging
from os import getenv
from typing import Literal, TypedDict, Any, List, Dict, Optional


class ColumnMetadata(TypedDict):
    colLabel: str  # Usually same as name
    colFormat: str  # Usually empty ''
    isEncrypted: bool  # Always false


class Columns(TypedDict):
    type: str
    name: str
    id: str  # Usually same as name
    visible: bool  # Always true
    metadata: ColumnMetadata


class Schema(TypedDict):
    columns: list[Columns]


class QueryResult(TypedDict):
    datasource: str
    columns: list[str]
    rows: list[list[Any]]


def combine_schemas(schema_a: Schema, schema_b: Schema) -> Schema:
    """
    Combines two Domo schemas, adding columns from schema_b to schema_a
    without replacing existing columns if names conflict.
    New columns from schema_b are added to the end.

    Args:
        schema_a (Schema): The base schema.
        schema_b (Schema): The schema with columns to add.

    Returns:
        Schema: A new schema with combined columns.
    """
    combined_columns = list(
        schema_a["columns"]
    )  # Start with a copy of schema_a's columns
    existing_column_names = {col["name"] for col in combined_columns}

    for col_b in schema_b["columns"]:
        if col_b["name"] not in existing_column_names:
            combined_columns.append(col_b)
            existing_column_names.add(col_b["name"])

    return Schema(columns=combined_columns)


class Domo:
    def __init__(self):
        self.base_url, self.headers = self.get_domo_details()
        self.logger = logging.getLogger("DOMO")

    @staticmethod
    def get_domo_details() -> tuple[str, dict[str, str]]:
        return f"https://{getenv('DOMO_INSTANCE')}.domo.com/api", {
            "X-DOMO-Developer-Token": getenv("DOMO_DEVELOPER_TOKEN", ""),
        }

    def infer_schema_from_df(self, df: pd.DataFrame) -> Schema:
        """
        Infers a Domo schema from a pandas DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to infer schema from.

        Returns:
            Schema: The inferred Domo schema.
        """
        domo_columns: list[Columns] = []
        for column_name, dtype in df.dtypes.items():
            str_column_name = str(column_name)
            domo_type = "STRING"  # Default type
            if pd.api.types.is_datetime64_any_dtype(dtype):
                domo_type = "DATETIME"
            elif pd.api.types.is_integer_dtype(dtype):
                domo_type = "LONG"
            elif pd.api.types.is_float_dtype(dtype):
                domo_type = "DOUBLE"
            elif pd.api.types.is_bool_dtype(dtype):
                domo_type = "STRING"  # Or LONG if you prefer 0/1
            elif pd.api.types.is_object_dtype(dtype):
                domo_type = "STRING"
            elif pd.api.types.is_categorical_dtype(dtype):
                domo_type = "STRING"

            column_metadata = ColumnMetadata(
                colLabel=str_column_name, colFormat="", isEncrypted=False
            )

            domo_columns.append(
                Columns(
                    type=domo_type,
                    name=str_column_name,
                    id=str_column_name,  # Use column name as id
                    visible=True,  # Default to visible
                    metadata=column_metadata,
                )
            )

        return Schema(columns=domo_columns)

    def query_dataset(self, dataset_id: str, query: str) -> QueryResult:
        """
        Query a dataset in Domo

        Args:
            dataset_id (str): The dataset ID
            query (str): The query to execute

        Returns:
            dict: The response from the query
        """
        try:
            url = f"{self.base_url}/query/v1/execute/{dataset_id}"
            data = {"sql": query}
            r = requests.post(url, headers=self.headers, json=data)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            self.logger.error(f"Error querying dataset.", exc_info=e)
            return QueryResult(datasource="", columns=[], rows=[])

    def get_latest_value(self, dataset_id: str, column: str, default: int = 0) -> int:
        """
        Get the latest value of a column in a dataset

        Args:
            dataset_id (str): The dataset ID
            column (str): The column to get the latest value from
            default (int): The default value to return if there is no data

        Returns:
            int: The latest value of the column
        """
        result = self.query_dataset(dataset_id, f"SELECT MAX({column}) FROM table")
        return result["rows"][0][0] or default

    def upload_df(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        action: Literal["APPEND", "REPLACE"] = "APPEND",
        chunk_size: int = 15000,
    ) -> None:
        """
        Upload a DataFrame to a Domo dataset

        Args:
            df (pd.DataFrame): The DataFrame to upload
            dataset_id (str): The dataset ID
            action (str): The action to perform on the dataset (APPEND, REPLACE, UPDATE)
            chunk_size (int): The number of rows to upload in each part

        Returns:
            None
        """
        rows = len(df)
        if rows == 0:
            self.logger.info("No data to upload to DOMO.")
            return

        # Get upload id
        url = f"{self.base_url}/data/v3/datasources/{dataset_id}/uploads"
        data = {"action": None, "appendId": None}
        r = requests.post(url, headers=self.headers, json=data)
        r.raise_for_status()
        upload_id = r.json()["uploadId"]
        self.logger.info(
            f"Preparing to upload data to DOMO. Rows: {rows} — Upload ID: {upload_id}"
        )

        # Upload data
        self.headers.update({"Content-Type": "text/csv"})
        part_number = 1

        for start in range(0, rows, chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            url = f"{self.base_url}/data/v3/datasources/{dataset_id}/uploads/{upload_id}/parts/{part_number}"
            data = chunk.to_csv(index=False, header=False).encode("utf-8")
            r = requests.put(url, headers=self.headers, data=data)
            r.raise_for_status()
            self.logger.info(f"Uploaded part {part_number}")
            part_number += 1

        # Commit upload
        self.headers.update({"Content-Type": "application/json"})
        url = f"{self.base_url}/data/v3/datasources/{dataset_id}/uploads/{upload_id}/commit"
        data = {"action": action, "index": True}
        r = requests.put(url, headers=self.headers, json=data)
        r.raise_for_status()
        self.logger.info(f"Data uploaded to DOMO with action: {action}.")

    def get_dataset_schema(self, dataset_id: str) -> Schema:
        """
        Get the schema of a dataset

        Args:
            dataset_id (str): The dataset ID

        Returns:
            Schema: The latest schema of the dataset
        """
        url = f"{self.base_url}/data/v2/datasources/{dataset_id}/schemas/latest"
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.json().get("schema")

    def update_dataset_schema(self, dataset_id: str, schema: Schema) -> None:
        """
        Update a dataset schema in Domo

        Args:
            dataset_id (str): The dataset ID
            schema (Schema): The schema of the dataset

        Returns:
            None
        """
        url = f"{self.base_url}/data/v2/datasources/{dataset_id}/schemas"
        r = requests.post(url, headers=self.headers, json=schema)
        r.raise_for_status()
        self.logger.info(f"Updated schema of dataset: {dataset_id}")

    def create_dataset(self, name: str, description: str, schema: Schema) -> str:
        """
        Create a new dataset in Domo

        Args:
            name (str): The name of the dataset
            description (str): The description of the dataset
            schema (Schema): The schema of the dataset

        Returns:
            str: The id of the created dataset
        """
        url = f"{self.base_url}/data/v2/datasources/"
        data = {
            "userDefinedType": "api",
            "dataSourceName": name,
            "description": description,
            "schema": schema,
        }
        r = requests.post(url, headers=self.headers, json=data)
        r.raise_for_status()
        dataset_id = r.json().get("dataSource").get("dataSourceId")
        self.logger.info(f"Dataset created with ID: {dataset_id}")
        return dataset_id

    def list_datasets(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get a list of all datasets from Domo

        Args:
            limit (int): Maximum number of datasets per page (default: 50)
            include_all (bool): Whether to retrieve all datasets through pagination (default: True)

        Returns:
            List[Dict[str, Any]]: List of dataset objects
        """
        if limit > 50:
            self.logger.warning("Limit is greater than 50, setting to 50")
            limit = 50

        url = f"{self.base_url}/data/v3/datasources"
        offset = 0
        all_datasets: list[Dict[str, Any]] = []

        try:
            self.logger.info("Starting dataset list retrieval...")
            while True:
                params = {"limit": limit, "offset": offset}

                r = requests.get(url, headers=self.headers, params=params)
                r.raise_for_status()

                response = r.json()
                datasets = response.get("dataSources", [])
                if len(datasets) == 0:
                    break

                all_datasets.extend(datasets)
                self.logger.info(
                    f"Retrieved {len(datasets)} datasets, offset: {offset}"
                )

                # Move to next page
                offset += limit

            self.logger.info(f"Retrieved a total of {len(all_datasets)} datasets")
            return all_datasets

        except Exception as e:
            self.logger.error(f"Error retrieving datasets: {e}")
            return []
