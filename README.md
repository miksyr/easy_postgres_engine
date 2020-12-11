# Easy Postgres Engine

The aim of this package is to provide an easier way to interface with Postgres databases.  

It contains a single _PostgresEngine_ class that extracts some typical features that a database connector would need.  Generating connections/cursors, and wrapping select/update queries in a retry decorator.

## Installation
```pip install easy_postgres_engine```

## Use

You can use it like this;
```
from easy_postgres_engine import PostgresEngine

engine = PostgresEngine(
    databaseName=DATABASE_NAME, 
    user=USER, 
    password=PASSWORD, 
    host=HOST, # default "localhost"
    port=PORT  # default 5432
   )
   
results = engine.run_select_query(
    query="SELECT * FROM tbl_example WHERE example_variable = %(exampleVariable)s",
    parameters={'exampleVariable': 100}
)
```

Or by subclassing and defining some commonly used queries;

```
from easy_postgres_engine import PostgresEngine

class ExampleEngine(PostgresEngine):

    def __init__(user, password, databaseName, host, port):
        super().__init__(user=user, password=password, databaseName=databaseName, host=host, port=port)

    def example_query(exampleVariable):
        return self.run_select_query(
            query="SELECT id FROM tbl_example WHERE example_variable = %(exampleVariable)s,
            parameters={'exampleVariable': exampleVariable}
        )
```
