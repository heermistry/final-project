import json
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from functools import wraps

# Verify and load cloud configuration
cloud_config_path = 'secure-connect-databasebigdatatool.zip'
if not os.path.exists(cloud_config_path):
    raise FileNotFoundError(f"Cloud configuration file not found: {cloud_config_path}")

cloud_config = {'secure_connect_bundle': cloud_config_path}

# Verify and load secrets
secrets_file = "databasebigdatatool-token.json"
if not os.path.exists(secrets_file):
    raise FileNotFoundError(f"Secrets file not found: {secrets_file}")

with open(secrets_file, 'r') as f:
    secrets = json.load(f)

# Set up Cassandra connection
auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Set keyspace
session.set_keyspace('salesdata')

# Define a decorator for Cassandra queries
def cassandra_query(query):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                prepared_query = session.prepare(query)
                return func(prepared_query, *args, **kwargs)
            except Exception as e:
                raise Exception(f"Error executing Cassandra query: {e}")
        return wrapper
    return decorator

# Table creation
table_queries = [
    """
    CREATE TABLE IF NOT EXISTS bronze (
        orderid INT PRIMARY KEY,
        itemid TEXT,
        orderunits FLOAT,
        city TEXT,
        state TEXT,
        zipcode TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS silver (
        orderid INT PRIMARY KEY,
        itemid TEXT,
        orderunits FLOAT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        category TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS gold (
        orderid INT PRIMARY KEY,
        itemid TEXT,
        orderunits FLOAT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        category TEXT,
        revenue FLOAT
    );
    """
]

for query in table_queries:
    try:
        session.execute(query)
    except Exception as e:
        raise Exception(f"Failed to create table: {e}")

# Verify and load JSON data
data_file = "Download messages as JSON_2024-11-23T23_25_27.633Z.json"
if not os.path.exists(data_file):
    raise FileNotFoundError(f"Data file not found: {data_file}")

with open(data_file, 'r') as f:
    try:
        data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Invalid data format: Expected a list of records")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to decode JSON data: {e}")

# Define data insertion functions
@cassandra_query("""
INSERT INTO bronze (orderid, itemid, orderunits, city, state, zipcode)
VALUES (?, ?, ?, ?, ?, ?)
""")
def insert_into_bronze(prepared_query, record):
    value = record.get("value", {})
    address = value.get("address", {})
    session.execute(prepared_query, [
        int(value.get("orderid", 0)),
        str(value.get("itemid", "")),
        float(value.get("orderunits", 0)),
        str(address.get("city", "")),
        str(address.get("state", "")),
        str(address.get("zipcode", ""))
    ])

@cassandra_query("""
INSERT INTO silver (orderid, itemid, orderunits, city, state, zipcode, category)
VALUES (?, ?, ?, ?, ?, ?, ?)
""")
def insert_into_silver(prepared_query, record):
    value = record.get("value", {})
    address = value.get("address", {})
    category = "premium" if float(value.get("orderunits", 0)) > 5 else "standard"
    session.execute(prepared_query, [
        int(value.get("orderid", 0)),
        str(value.get("itemid", "")),
        float(value.get("orderunits", 0)),
        str(address.get("city", "")),
        str(address.get("state", "")),
        str(address.get("zipcode", "")),
        category
    ])

@cassandra_query("""
INSERT INTO gold (orderid, itemid, orderunits, city, state, zipcode, category, revenue)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")
def insert_into_gold(prepared_query, record):
    value = record.get("value", {})
    address = value.get("address", {})
    category = "premium" if float(value.get("orderunits", 0)) > 5 else "standard"
    revenue = float(value.get("orderunits", 0)) * 100  # Assume revenue is orderunits * 100
    session.execute(prepared_query, [
        int(value.get("orderid", 0)),
        str(value.get("itemid", "")),
        float(value.get("orderunits", 0)),
        str(address.get("city", "")),
        str(address.get("state", "")),
        str(address.get("zipcode", "")),
        category,
        revenue
    ])

# Insert data
for record in data:
    try:
        insert_into_bronze(record)
        insert_into_silver(record)
        insert_into_gold(record)
    except Exception as e:
        print(f"Error inserting record: {e}")

print("Data inserted into Cassandra database successfully.")



