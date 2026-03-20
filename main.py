from pathlib import Path

import pandas as pd
from influxdb import DataFrameClient
from influxdb import InfluxDBClient

# First - tunnel the port, so script can reach the db
# (This is done from another terminal)
# ssh -N -L 8086:127.0.0.1:8086 user@server

# Connection config
HOST = '127.0.0.1'
PORT = 8086
DATABASE = 'boatdata'
MMSI = '211891460'
CONTEXT = f'vessels.urn:mrn:imo:mmsi:{MMSI}'

YEARS    = range(2022, 2024)
OUTPUT   = Path('output')
OUTPUT.mkdir(exist_ok=True)

# Bounding boxes for the areas
AREAS = [
    {
        'name': 'Kiel_Fjord',
        'lat_min': 54.312117,
        'lat_max': 54.456069,
        'lon_min': 10.130081,
        'lon_max': 10.309982
    },
    {
        'name': 'Kiel_Canal',
        'lat_min': 53.883297,
        'lat_max': 54.373758,
        'lon_min': 9.077005,
        'lon_max': 10.130368
    },
]

# Helper data formatting function
def cast_to_df(dates):
    points = list(dates.get_points())
    if not points:
        return pd.DataFrame()
    df = pd.DataFrame(points)
    df['time'] = pd.to_datetime(df['time'], format='ISO8601', utc=True)
    df = df.set_index('time').sort_index()
    return df

# Connect
client = InfluxDBClient(host=HOST, port=PORT, database=DATABASE)

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

# Get the data for each area for every year
for year in YEARS:
    print(f"\nYEAR: {year}")

    start = f"{year}-01-01T00:00:00Z"
    end = f"{year + 1}-01-01T00:00:00Z"

    result = client.query(f'''
        SELECT lat, lon
        FROM "navigation.position"
        WHERE context = '{CONTEXT}'
        AND time >= '{start}' AND time < '{end}'
        ORDER BY time ASC
    ''')


    df = cast_to_df(result)

    if df.empty:
        print(f"  No data for {year}, skipping.")
        continue

    print(f"  Fetched {len(df):,} rows")
    
     # Filter by area
    area_frames = []
    for area in AREAS:
        mask = (
                (df['lat'] > area['lat_min']) & (df['lat'] < area['lat_max']) &
                (df['lon'] > area['lon_min']) & (df['lon'] < area['lon_max'])
        )
        filtered = df[mask].copy()
        filtered['area'] = area['name']
        area_frames.append(filtered)
        print(f"  {area['name']}: {len(filtered):,} rows matched")

    # Combine both areas for this year
    year_df = pd.concat(area_frames).sort_index()

    if year_df.empty:
        print(f"  No rows matched any area for {year}, skipping CSV")
        continue

    # Export
    out_path = OUTPUT / f"wavelab_{year}.csv"
    year_df.to_csv(out_path, index=True, index_label='time')
    print(f"  Saved to the {out_path}  ({len(year_df):,} rows total)")

print("\n FINISHED")