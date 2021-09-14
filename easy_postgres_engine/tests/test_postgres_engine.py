import testing.postgresql
from unittest import TestCase

from .test_table_schema import TEST_TABLE_SCHEMA
from ..postgres_engine import PostgresEngine


def create_table(postgresqlConnection):
    config = postgresqlConnection.dsn()
    dbEngine = PostgresEngine(
        databaseName=config["database"],
        user=config["user"],
        password=config.get("password"),
        port=config["port"],
        host=config["host"],
    )
    dbEngine.create_table(schema=TEST_TABLE_SCHEMA)


# Generate Postgresql class which shares the generated database
Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True, on_initialized=create_table)


def tearDownModule():
    # clear cached database at end of tests
    Postgresql.clear_cache()


class TestPostgresEngine(TestCase):
    def __init__(self, methodName="runTest"):
        super(TestPostgresEngine, self).__init__(methodName=methodName)
        self.firstCustomerId = 10
        self.firstCustomerName = "Mary"
        self.secondCustomerId = 50
        self.secondCustomerName = "John"

    def setUp(self):
        super().setUp()
        self.postgresql = Postgresql()
        config = self.postgresql.dsn()
        self.dbEngine = PostgresEngine(
            databaseName=config["database"],
            user=config["user"],
            password=config.get("password"),
            port=config["port"],
            host=config["host"],
        )

    def tearDown(self):
        super().tearDown()
        self.postgresql.stop()

    def test_engine(self):
        # TODO(Mike): probably better to test insertion + selection queries separately, but DB persistence between test functions is awkward
        TEST_INSERTION_QUERY = """
            INSERT INTO 
                tbl_example(customer_id, customer_name)
            VALUES
                (%(customerId)s, %(customerName)s)
        """
        insertedId1 = self.dbEngine.run_update_query(
            query=TEST_INSERTION_QUERY, parameters={"customerId": self.firstCustomerId, "customerName": self.firstCustomerName}
        )
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.run_update_query(
            query=TEST_INSERTION_QUERY,
            parameters={"customerId": self.secondCustomerId, "customerName": self.secondCustomerName},
        )
        self.assertEqual(insertedId2, 2)

        queryResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_example")
        self.assertSequenceEqual(list(queryResults["customer_id"].values), [self.firstCustomerId, self.secondCustomerId])
        self.assertSequenceEqual(list(queryResults["customer_name"].values), [self.firstCustomerName, self.secondCustomerName])

        specificQueryResults = self.dbEngine.run_select_query(
            query="""
                SELECT
                    *
                FROM
                    tbl_example
                WHERE
                    customer_id = %(customerId)s
            """,
            parameters={"customerId": self.firstCustomerId},
        )
        self.assertEqual(len(specificQueryResults), 1)
        self.assertEqual(specificQueryResults["customer_name"].iloc[0], self.firstCustomerName)
