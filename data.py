import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Cassandra connection details
cloud_config_path = 'secure-connect-databasebigdatatool.zip'
secrets_file = "databasebigdatatool-token.json"

# Load secrets
import json
with open(secrets_file, 'r') as f:
    secrets = json.load(f)

# Connect to Cassandra
auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud={'secure_connect_bundle': cloud_config_path}, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('salesdata')

# Fetch data from the gold table
query = "SELECT orderid, itemid, orderunits, city, state, zipcode, category, revenue FROM gold"
rows = session.execute(query)

# Write to a CSV file
output_file = "gold_data.csv"

with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # Write header
    writer.writerow(['orderid', 'itemid', 'orderunits', 'city', 'state', 'zipcode', 'category', 'revenue'])
    # Write rows
    for row in rows:
        writer.writerow([row.orderid, row.itemid, row.orderunits, row.city, row.state, row.zipcode, row.category, row.revenue])

print(f"Data successfully exported to {output_file}")
