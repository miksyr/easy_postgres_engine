import logging
import pandas as pd
import psycopg2
import psycopg2.extras

from retry_decorator import retry


class PostgresEngine:

    def __init__(self, databaseName, user, password, host='localhost', port=5432):
        self.databaseName = databaseName
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def _get_connection(self):
        try:
            self.connection = psycopg2.connect(user=self.user, password=self.password, host=self.host, port=self.port, database=self.databaseName)
        except Exception as ex:
            logging.exception(f'Error connecting to PostgreSQL {ex}')
            raise ex

    def _get_cursor(self, isInsertionQuery):
        if isInsertionQuery:
            self.cursor = self.connection.cursor()
        else:
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def _close_connection(self):
        self.connection.close()

    def _close_cursor(self):
        self.cursor.close()

    def close(self):
        if self.connection is not None:
            self._close_connection()
        if self.cursor is not None:
            self._close_cursor()

    def create_table(self, schema):
        self._get_connection()
        self._get_cursor(isInsertionQuery=True)
        self.cursor.execute(schema)
        try:
            self.connection.commit()
        except Exception as ex:
            logging.exception(f'error: {ex} \nschemaQuery: {schema}')
            raise ex
        finally:
            self.close()

    def create_index(self, tableName, column):
        self._get_connection()
        self._get_cursor(isInsertionQuery=True)
        indexQuery = f'CREATE INDEX IF NOT EXISTS {tableName}_{column} ON {tableName}({column});'
        self.cursor.execute(indexQuery)
        try:
            self.connection.commit()
        except Exception as ex:
            logging.exception(f'error: {ex} \nindexQuery: {indexQuery}')
            raise ex
        finally:
            self.close()

    def create_new_foreign_key_constraint(self, tableName, constraintName, foreignKeySQL):
        FOREIGN_KEY_QUERY = """
            ALTER TABLE {tableName} 
            ADD CONSTRAINT {constraintName} {foreignKeySQL};
        """
        self.create_table(
            schema=FOREIGN_KEY_QUERY.format(
                tableName=tableName,
                constraintName=constraintName,
                foreignKeySQL=foreignKeySQL
            )
        )

    @retry(numRetries=5, retryDelay=3, backoffScalingFactor=2)
    def run_select_query(self, query, parameters=None):
        self._get_connection()
        self._get_cursor(isInsertionQuery=False)
        self.cursor.execute(query, parameters)
        outputs = self.cursor.fetchall()
        self.close()
        outputDataframe = pd.DataFrame(outputs)
        return outputDataframe.where(outputDataframe.notnull(), None).dropna(axis=0, how='all')

    @retry(numRetries=5, retryDelay=3, backoffScalingFactor=2)
    def run_update_query(self, query, parameters=None, returnId=True):
        self._get_connection()
        self._get_cursor(isInsertionQuery=True)
        if returnId:
            query = f'{query}\nRETURNING id'
        self.cursor.execute(query, parameters)
        if returnId:
            insertedId = self.cursor.fetchone()[0]
        else:
            insertedId = None
        try:
            self.connection.commit()
        except Exception as ex:
            logging.exception(f'error: {ex} \nquery: {query} \nparameters: {parameters}')
            raise ex
        finally:
            self.close()
        return insertedId
