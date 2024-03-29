import logging
from typing import Any, Dict, Optional, Union

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

from .retry_decorator import retry


def replace_nan_with_none_in_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.where(dataframe.notnull(), None).dropna(axis=0, how="all")
    return dataframe.replace({np.nan: None})


class PostgresEngine:
    def __init__(self, databaseName: str, user: str, password: str, host: str = "localhost", port: int = 5432):
        """
        Class for accessing Postgres databases more easily.

        :param databaseName (str): the name of the database to connect to
        :param user (str): the user to log in as
        :param password (str): password of the user
        :param host (str): host address to connect to
        :param port (int): port where the database is available
        """
        self.databaseName = databaseName
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def _get_connection(self) -> None:
        try:
            self.connection = psycopg2.connect(
                user=self.user, password=self.password, host=self.host, port=self.port, database=self.databaseName
            )
        except Exception as ex:
            logging.exception(f"Error connecting to PostgreSQL {ex}")
            raise ex

    def _get_cursor(self, isInsertionQuery: bool) -> None:
        if isInsertionQuery:
            self.cursor = self.connection.cursor()
        else:
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def _close_connection(self) -> None:
        self.connection.close()

    def _close_cursor(self) -> None:
        self.cursor.close()

    def close(self) -> None:
        if self.connection is not None:
            self._close_connection()
        if self.cursor is not None:
            self._close_cursor()

    def create_table(self, schema: str) -> None:
        self._get_connection()
        self._get_cursor(isInsertionQuery=True)
        self.cursor.execute(schema)
        try:
            self.connection.commit()
        except Exception as ex:
            logging.exception(f"error: {ex} \nschemaQuery: {schema}")
            raise ex
        finally:
            self.close()

    def create_index(self, tableName: str, column: str) -> None:
        self._get_connection()
        self._get_cursor(isInsertionQuery=True)
        indexQuery = f"CREATE INDEX IF NOT EXISTS {tableName}_{column} ON {tableName}({column});"
        self.cursor.execute(indexQuery)
        try:
            self.connection.commit()
        except Exception as ex:
            logging.exception(f"error: {ex} \nindexQuery: {indexQuery}")
            raise ex
        finally:
            self.close()

    @retry(numRetries=5, retryDelaySeconds=3, backoffScalingFactor=2)
    def run_select_query_with_retry(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        return self.run_select_query(query=query, parameters=parameters)

    def run_select_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        self._get_connection()
        self._get_cursor(isInsertionQuery=False)
        self.cursor.execute(query, parameters)
        outputs = self.cursor.fetchall()
        self.close()
        outputDataframe = pd.DataFrame(outputs)
        return replace_nan_with_none_in_dataframe(dataframe=outputDataframe)

    @retry(numRetries=5, retryDelaySeconds=3, backoffScalingFactor=2)
    def run_update_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None, returnId: bool = True
    ) -> Union[None, int]:
        self._get_connection()
        self._get_cursor(isInsertionQuery=True)
        if returnId:
            query = f"{query}\nRETURNING id"
        try:
            self.cursor.execute(query, parameters)
            if returnId:
                insertedId = self.cursor.fetchone()[0]
            else:
                insertedId = None
            self.connection.commit()
        except Exception as ex:
            logging.exception(f"error: {ex} \nquery: {query} \nparameters: {parameters}")
            raise ex
        finally:
            self.close()
        return insertedId
