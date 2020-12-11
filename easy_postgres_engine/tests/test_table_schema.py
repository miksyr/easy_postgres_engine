TEST_TABLE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS tbl_example
        (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER,
            customer_name TEXT
        );
"""
