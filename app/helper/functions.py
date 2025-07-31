import folium
import os
from shapely import LineString
from shapely.wkt import loads
from helper.parameters import NAMESPACES  # Import NAMESPACES here
import re

def extract_route_name(file_path):
    """Extracts the route name from the filename using '-' or '_' as separators."""
    filename = os.path.basename(file_path)

    # ✅ Split using both '-' and '_'
    parts = re.split(r"[-_]", filename)

    return parts[0] if parts else filename  # Return first part or full filename if no match


def extract_days_of_week(journey):
    all_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekdays = all_days[:5]
    weekend = all_days[5:]

    days_elements = journey.findall('.//txc:DaysOfWeek/*', NAMESPACES)
    day_tags = [el.tag.split('}')[-1] for el in days_elements]

    if not day_tags:
        return ', '.join(all_days)

    if 'MondayToFriday' in day_tags:
        return ', '.join(weekdays)
    if 'Weekend' in day_tags:
        return ', '.join(weekend)
    if 'MondayToSunday' in day_tags or 'Everyday' in day_tags:
        return ', '.join(all_days)

    return ', '.join(day_tags)

def extract_raw_days(journey):

    tags = journey.findall('.//txc:RegularDayType//*', NAMESPACES)

    # Include only tags with no children (i.e. leaf nodes)
    tag_names = [el.tag.split('}')[-1] for el in tags if len(el) == 0]

    return ', '.join(tag_names) if tag_names else None

def calculate_total_distance(route_sections_df, start_stop, end_stop):
    total_distance = 0
    current_stop = start_stop

    while current_stop != end_stop:
        # Find the route link starting from the current stop
        next_link = route_sections_df[route_sections_df['From'] == current_stop]

        if next_link.empty:
            print(f"No route found starting from Stop Point: {current_stop}")
            break

        # Get the next stop and distance
        current_stop = next_link.iloc[0]['To']
        distance = next_link.iloc[0]['Distance']

        if distance is not None:
            total_distance += float(distance)

        # Break if the loop is infinite (safety check)
        if current_stop == start_stop:
            print("Infinite loop detected.")
            break

    return total_distance


def add_line_to_map(df):
    m = folium.Map(location=[53.479777386680375, -2.235138061840938], zoom_start=12)
    for _, row in df.iterrows():
        if row["LineString"]:
            line = loads(row["LineString"])  # Convert WKT back to geometry
            folium.PolyLine([(p[1], p[0]) for p in line.coords], color="blue").add_to(m)

    m.save("map.html")  # Open this file in a browser


def get_text(element, path):
    if element is None:
        return None
    if path.startswith('@'):  # Handle attributes
        return element.attrib.get(path[1:], None)
    found_element = element.find(path, NAMESPACES)
    return found_element.text if found_element is not None else None


def get_coordinates(element):
    """Extracts latitude and longitude from a Location element."""
    if element is None:
        return None, None
    lat = get_text(element, 'txc:Latitude')
    lon = get_text(element, 'txc:Longitude')
    return lat, lon


def get_linestring(link):
    """Extracts latitude and longitude from all <Location> elements in a RouteLink and returns a LineString (WKT)."""
    locations = link.findall('.//txc:Location', NAMESPACES)  # Get all Location elements inside RouteLink
    #locations = link.findall('.//txc:Translation', NAMESPACES)

    coordinates = []
    for loc in locations:
        lat, lon = get_text(loc, 'txc:Latitude'), get_text(loc, 'txc:Longitude')

        # Ensure values exist and are valid floats
        if lat and lon:
            try:
                coordinates.append((float(lon), float(lat)))  # Swap lat/lon order for GIS compatibility
            except ValueError:
                print(f"Invalid coordinates found: {lat}, {lon}")

    if len(coordinates) >= 2:  # Ensure at least a line (not just a point)
        return LineString(coordinates).wkt
    return None  # Return None if there's not enough data


def handle_distance(series):
    """Return empty string if any value is None, otherwise sum the values."""
    if series.isnull().any():
        return None
    return series.sum()


def compute_arrival_time(departure, duration):
    """Returns arrival time in HH:MM:SS format given departure time and duration in seconds."""
    hours, minutes, seconds = map(int, departure.split(":"))

    # Convert departure time to total seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds

    # Add duration
    total_seconds += duration

    # Convert back to HH:MM:SS format
    new_hours = total_seconds // 3600
    new_minutes = (total_seconds % 3600) // 60
    new_seconds = total_seconds % 60

    return f"{new_hours:02}:{new_minutes:02}:{new_seconds:02}"
