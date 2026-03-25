from pathlib import Path

import pandas as pd
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

# Split extras for dynamic entries (query iteratively)
EXTRA_DYNAMIC = {
    "navigation.courseOverGroundTrue": "value",
}

# Static entries (queried once via context and propagated)
EXTRA_STATIC = {
    "registrations": "jsonValue",
    "design.aisShipType": "jsonValue",
    "name": "stringValue",
}

# Helper data formatting function
def cast_to_df(dates):
    points = list(dates.get_points())
    if not points:
        return pd.DataFrame()
    dframe = pd.DataFrame(points)
    dframe['time'] = pd.to_datetime(dframe['time'], format='ISO8601', utc=True)
    dframe = dframe.set_index('time').sort_index()
    return dframe

# Connect
client = InfluxDBClient(host=HOST, port=PORT, database=DATABASE)

# Get the data for each area for every year
for year in YEARS:
    print(f"\nYEAR: {year}")

    start = f"{year}-01-01T00:00:00Z"
    end = f"{year + 1}-01-01T00:00:00Z"

    result = client.query(f'''
        SELECT lat, lon, context
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

    # Query static metadata
    static_meta = {}
    for measurement, col in EXTRA_STATIC.items():
        result = client.query(f'''
            SELECT {col}
            FROM "{measurement}"
            WHERE context = '{CONTEXT}'
            LIMIT 1
        ''')
        points = list(result.get_points())
        if points:
            static_meta[measurement] = points[0][col]
        else:
            static_meta[measurement] = None

    print("Static metadata:", static_meta)
    for measurement, value in static_meta.items():
        df[measurement] = value

    # Extra measurements
    for measurement, col in EXTRA_DYNAMIC.items():
        extra_result = client.query(f'''
            SELECT {col}
            FROM "{measurement}"
            WHERE context = '{CONTEXT}'
            AND time >= '{start}' AND time < '{end}'
            ORDER BY time ASC
        ''')

        extra_df = cast_to_df(extra_result)
        if not extra_df.empty:
            extra_df = extra_df[[col]].rename(columns={col: measurement})

            # Reset index for merge_asof (needs a column, not an index)
            df_reset = df.reset_index()
            extra_reset = extra_df.reset_index()

            merged = pd.merge_asof(
                df_reset,
                extra_reset[['time', measurement]],
                on='time',
                direction='backward'  # use the last known value
            )
            df = merged.set_index('time')
    
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