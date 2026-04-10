# Connection & Export config
from pathlib import Path

HOST = '127.0.0.1'
PORT = 8086
DATABASE = 'boatdata'
MMSI = '211891460'
CONTEXT = f'vessels.urn:mrn:imo:mmsi:{MMSI}'
START = f"2023-09-19T00:00:00Z"
END = f"2023-09-19T23:59:00Z"

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
    "navigation.state":"stringValue"
}

# Static entries (queried once via context and propagated)
EXTRA_STATIC = {
    "registrations": "jsonValue",
    "design.1": "jsonValue",
    "name": "stringValue",
}