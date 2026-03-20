from influxdb import DataFrameClient

# Connection config
HOST = '127.0.0.1'
PORT = 8086
DATABASE = 'boatdata'
MMSI = '211891460'
CONTEXT = f'vessels.urn:mrn:imo:mmsi:{MMSI}'

# Connect
client = DataFrameClient(host=HOST, port=PORT, database=DATABASE)

# Reachability check
dbs = client.get_list_database()
print("Databases found:", [d['name'] for d in dbs])

# Sample query
result = client.query(f'''
    SELECT time, lat, lon
    FROM "navigation.position"
    WHERE context = '{CONTEXT}'
    ORDER BY time ASC
    LIMIT 10
''')

if 'navigation.position' not in result:
    print(f"No position data returned for MMSI: {MMSI}")
else:
    df = result['navigation.position']
    print(f"\nShape: {df.shape}")
    print(f"Time range: {df.index.min()} → {df.index.max()}")
    print(f"\nFirst rows:\n{df.head()}")
