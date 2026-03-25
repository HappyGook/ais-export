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

# Helper function to gather statics & extras
def gather_extras(client, context, extra_static, extra_dynamic, start=None, end=None):

    static_meta = {}
    for measurement, col in extra_static.items():
        result = client.query(f'''
            SELECT {col}
            FROM "{measurement}"
            WHERE context = '{context}'
            ORDER BY time DESC
            LIMIT 1
        ''')
        points = list(result.get_points())
        static_meta[measurement] = points[0][col] if points else None

    dynamic_df = pd.DataFrame()
    if start and end:
        frames = {}
        for measurement, col in extra_dynamic.items():
            result = client.query(f'''
                SELECT {col}
                FROM "{measurement}"
                WHERE context = '{context}'
                AND time >= '{start}' AND time < '{end}'
                ORDER BY time ASC
            ''')
            extra_df = cast_to_df(result)
            if not extra_df.empty:
                frames[measurement] = extra_df[[col]].rename(columns={col: measurement})

        if frames:
            dynamic_df = pd.concat(frames.values(), axis=1)

    return static_meta, dynamic_df

# Add the gathered extras to the dataframe
def apply_extras(df, static_meta, dynamic_df):

    for measurement, value in static_meta.items():
        df[measurement] = value

    if not dynamic_df.empty:
        df_reset = df.reset_index()
        dyn_reset = dynamic_df.reset_index()

        for col in dynamic_df.columns:
            df_reset = pd.merge_asof(
                df_reset,
                dyn_reset[['time', col]],
                on='time',
                direction='backward'
            )
        df = df_reset.set_index('time')

    return df

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

    static_meta, dynamic_df = gather_extras(client, CONTEXT, EXTRA_STATIC, EXTRA_DYNAMIC, start, end)

    df = apply_extras(df, static_meta, dynamic_df)
    
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