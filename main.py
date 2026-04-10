import math
import config
import pandas as pd
from influxdb import InfluxDBClient

# First - tunnel the port, so script can reach the db
# (This is done from another terminal)
# ssh -N -L 8086:127.0.0.1:8086 user@server



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

# Function that defines the dynamic bounding box
# 1 deg lat as 111.32 km; 1 deg lon 111.32*cos(lat) km
def get_dynamic_bbox(pos_lat, pos_lon):
    lat_offset = 1.0 / 111.32
    lon_offset = 1.0 / (111.32 * math.cos(math.radians(pos_lat)))
    return {
        'lat_min': pos_lat - lat_offset,
        'lat_max': pos_lat + lat_offset,
        'lon_min': pos_lon - lon_offset,
        'lon_max': pos_lon + lon_offset,
    }

# Connect
client = InfluxDBClient(host=config.HOST, port=config.PORT, database=config.DATABASE)

# Get the data for the given period
result = client.query(f'''
    SELECT lat, lon, context
    FROM "navigation.position"
    WHERE context = '{config.CONTEXT}'
    AND time >= '{config.START}' AND time < '{config.END}'
    ORDER BY time ASC
''')

df = cast_to_df(result)

if df.empty:
    print(f"  No data for the given period, skipping.")

print(f"  Fetched {len(df):,} rows")

static_meta, dynamic_df = gather_extras(client, config.CONTEXT, config.EXTRA_STATIC, config.EXTRA_DYNAMIC, config.START,
                                        config.END)

df = apply_extras(df, static_meta, dynamic_df)

# Filtering out the moored states
df = df[df['navigation.state'] != 'moored']
if df.empty:
    print(f"  All rows were moored for the given period, skipping.")

# Filter by area
area_frames = []
for area in config.AREAS:
    mask = (
            (df['lat'] > area['lat_min']) & (df['lat'] < area['lat_max']) &
            (df['lon'] > area['lon_min']) & (df['lon'] < area['lon_max'])
    )
    filtered = df[mask].copy()
    filtered['area'] = area['name']
    area_frames.append(filtered)
    print(f"  {area['name']}: {len(filtered):,} rows matched")

# Combine both areas
year_df = pd.concat(area_frames).sort_index()

if year_df.empty:
    print(f"  No rows matched any area for the given period, skipping CSV")

companion_frames = []
extras_cache = {}
checked_positions = set()

wl_counter = 0
for ts, wl_row in year_df.iterrows():
    wl_counter += 1
    completion = (wl_counter - 1) / len(year_df) * 100
    print(f"============ Timestamp No. {wl_counter} ({completion}% finished): {ts} ===========\n"
          f"wavelab infos at timestamp: {wl_row}")
    state = wl_row['navigation.state']
    if pd.isna(state) or state == "moored": continue
    bbox = get_dynamic_bbox(wl_row['lat'], wl_row['lon'])

    time_start = (ts - pd.Timedelta('30s')).strftime('%Y-%m-%dT%H:%M:%SZ')
    time_end = (ts + pd.Timedelta('30s')).strftime('%Y-%m-%dT%H:%M:%SZ')

    comp_result = client.query(f'''
                    SELECT lat, lon, context
                    FROM "navigation.position"
                    WHERE 
                    lon < {bbox['lon_max']} AND lon > {bbox['lon_min']} 
                    AND
                    lat < {bbox['lat_max']} AND lat > {bbox['lat_min']} 
                    AND
                    time >= '{time_start}' AND time < '{time_end}'
                    ORDER BY time ASC
                ''')

    comp_df = cast_to_df(comp_result)
    comp_df = comp_df[
        [(ctx, name) not in checked_positions
         for ctx, name in zip(comp_df['context'], comp_df.index)]
    ]
    checked_positions.update(zip(comp_df['context'], comp_df.index))

    print(f"  {len(comp_df):,} companion rows matched")

    if comp_df.empty:
        continue

    added_ships = []
    counter = 0
    for context in comp_df['context'].unique():
        counter += 1
        print(f"Gathering extras for {context}. \n "
              f"This is the unique entry number: {counter}")
        sub = comp_df[comp_df['context'] == context].copy()
        if context not in extras_cache:
            extras_cache[context] = gather_extras(
                client, context, config.EXTRA_STATIC, config.EXTRA_DYNAMIC, time_start, time_end
            )
        comp_static, comp_dynamic = extras_cache[context]
        sub = apply_extras(sub, comp_static, comp_dynamic)
        added_ships.append(sub)

    comp_df = pd.concat(added_ships).sort_index()

    companion_frames.append(comp_df)

all_frames = [year_df] + companion_frames
year_df = pd.concat(all_frames).sort_index()

# Export
out_path = config.OUTPUT / f"wavelab_{config.START}-{config.END}.csv"
year_df.to_csv(out_path, index=True, index_label='time')
print(f"  Saved to the {out_path}  ({len(year_df):,} rows total)")

print("\n FINISHED")