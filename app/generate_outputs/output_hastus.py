from helper.parameters import *
from helper.functions import *
import pandas as pd
from datetime import datetime, timedelta
from shapely import wkt
import geopandas as gpd
from fastkml import kml
from fastkml.kml import Document, Folder, Placemark


def make_kml_document(variant_gdf: gpd.GeoDataFrame) -> kml.KML:
    """Build KML Document from merged variant GeoDataFrame."""
    k = kml.KML()
    doc = Document()
    k.append(doc)

    for _, row in variant_gdf.iterrows():
        pm = kml.Placemark(name=row['Name'], geometry=row['Path'])
        desc = f"""
        <![CDATA[
        <html><body><p>{row['Description']}</p><table border="1">
        <tr><td>Route</td><td>{row['LineName']}</td></tr>
        <tr><td>VariantCode</td><td>{row['VariantCode']}</td></tr>
        <tr><td>Direction</td><td>{row['Direction'].title()}</td></tr>
        <tr><td>DirectionDescription</td><td>{row['DirectionDescription']}</td></tr>
        <tr><td>FirstStopId</td><td>{row['FirstStop']}</td></tr>
        <tr><td>LastStopId</td><td>{row['LastStop']}</td></tr>
        <tr><td>FirstName</td><td>{row['FirstName']}</td></tr>
        <tr><td>LastName</td><td>{row['LastName']}</td></tr>
        <tr><td>SuppliedDistance</td><td>{row['Distance']}</td></tr>
        <tr><td>MeasuredDistance</td><td>{row['MeasuredLength']}</td></tr>
        <tr><td>Discrepancy</td><td>{row['Discrepancy']}</td></tr>
        <tr><td>MissingLinks</td><td>{row['MissingPath']}</td></tr>
        </table></body></html>
        ]]>
        """
        pm.description = desc
        doc.append(pm)

    return k


def create_link_outputs(variant_links: pd.DataFrame, variant_points: pd.DataFrame, output_dir: str):
    """Generate merged KML output of variant paths, auto-building missing paths from stops."""

    output_path = f'{output_dir}/{region}/{region}.kml'

    variant_links['Path'] = variant_links['Path'].apply(
        lambda x: wkt.loads(x) if isinstance(x, str) else x
    )

    gdf = gpd.GeoDataFrame(variant_links, geometry='Path', crs=SOURCE_CRS).to_crs(TARGET_CRS)
    gdf['MeasuredLength'] = gdf.length
    gdf['Discrepancy'] = gdf['MeasuredLength'] - gdf['Distance']
    gdf['AbsDistanceDiscrepancy'] = gdf['Discrepancy'].round(0).abs()
    gdf['MissingPath'] = gdf['Path'].is_empty

    # --- Step 3: Summarize routes
    summary = gdf.groupby('VariantCode').agg({
        'LineName': 'first', 'Distance': 'sum', 'MeasuredLength': 'sum',
        'Direction': 'first', 'MissingPath': 'any', 'Description': 'first'
    }).reset_index()

    summary['Name'] = "Route: " + summary['LineName'] + ' - ' + summary['Direction'].str.title() + ' - Variant: ' + summary['VariantCode']
    summary['Discrepancy'] = (summary['MeasuredLength'] - summary['Distance']).round(0).astype(int)
    summary['MeasuredLength'] = summary['MeasuredLength'].round(0).astype(int)

    # --- Step 4: Add first/last stop information
    stops_info = variant_points.groupby('VariantCode').agg(
        FirstStop=('StopPointId', 'first'),
        LastStop=('StopPointId', 'last'),
        FirstName=('CommonName', 'first'),
        LastName=('CommonName', 'last')
    ).reset_index()

    summary = summary.merge(stops_info, on='VariantCode', how='left')
    summary['DirectionDescription'] = summary['Description']
    summary['Description'] = summary.apply(
        lambda x: f"{x['Direction'].title()} - From {x['FirstName']} To {x['LastName']}", axis=1
    )

    # --- Step 5: Merge paths into one geometry per variant
    merged_paths = gdf.groupby('VariantCode')['Path'].apply(lambda x: x.unary_union).reset_index()
    merged = merged_paths.merge(summary, on='VariantCode')

    merged = gpd.GeoDataFrame(merged, geometry='Path', crs=TARGET_CRS).to_crs(SOURCE_CRS)
    merged['RouteSort'] = merged['LineName'].str.extract(r'(\d+)')[0].astype(float)
    merged = merged.sort_values(['RouteSort', 'Name'])

    # --- Step 6: Generate KML
    kml_doc = make_kml_document(merged)

    # --- Step 7: Save to file
    with open(output_path, 'w') as f:
        f.write(kml_doc.to_string(prettyprint=True))

def add_places(table, stops):
    table = table.copy(deep =True)
    table = table.merge(stops[['StopPointId','Place']], left_on='FromStopPointId', right_on = 'StopPointId', how='left')
    table = table.rename(columns={'Place':'FromPlace'})
    table = table.drop(columns=['StopPointId'])
    table = table.merge(stops[['StopPointId','Place']], left_on='ToStopPointId', right_on = 'StopPointId', how='left')
    table = table.rename(columns={'Place':'ToPlace'})
    table = table.drop(columns=['StopPointId'])

    return table

def hastus_rte_version(variant_points, subdir):

    variant_points = variant_points.copy(deep=True)
    variant_points = variant_points.sort_values(['DataId','Direction','VariantCode','RouteSectionPosition', 'RouteLinkPosition']).reset_index(drop=True)
    variant_points = variant_points.drop_duplicates(subset=['Direction','VariantCode','JourneyPatternTimingLinkPositionInJourneyPattern'], keep='first')

    variant_points['Direction'] = variant_points['Direction'].map({'inbound':'4','outbound':'5'})
    variant_points["InboundDescriptionShort"] = variant_points["InboundDescriptionShort"].fillna(
        variant_points["DestinationDisplay"])
    variant_points["OutboundDescriptionShort"] = variant_points["OutboundDescriptionShort"].fillna(
        variant_points["DestinationDisplay"])

    # Map 'Direction' to 'Description'
    variant_points['RouteDescription'] = variant_points.apply(
        lambda row: row['InboundDescriptionShort'][:20] if row['Direction'] == 4
        else row['OutboundDescriptionShort'][:20],
        axis=1
    )

    lines = []

    lines.append(f"route_version|{rte_version}|{region} {description}")

    for route, route_group in variant_points.groupby('LineName'):
        # Get route-wide details
        direction = route_group['Direction'].iloc[0] if 'Direction' in route_group.columns else ''
        route_desc = route_group['RouteDescription'].iloc[0] if 'RouteDescription' in route_group.columns else ''

        # Add the route header once per route
        lines.append(f"route|{rte_version}|{service_type}|{route}|{route_desc}|{direction}|{service_mode}|{smoothing}|{route}")

        # Now group by Variant within each Route
        for variant, variant_group in route_group.groupby('VariantCode'):
            direction = variant_group['Direction'].iloc[0] if 'Direction' in variant_group.columns else ''
            variant_id = variant_group['VariantCode'].iloc[0] if 'VariantCode' in variant_group.columns else ''
            variant_desc = variant_group['CommonName'].iloc[0][:20] + ' to ' + variant_group['CommonName'].iloc[-1][:20]

            # Add variant header
            lines.append(f"rvariant|{route}|{direction}|{variant_id}|{variant_desc}|{variant_id}")

            # Iterate through variant points
            for _, point in variant_group.iterrows():
                stop = point['StopPointId'][4:]
                TP1 = '1' if point.get('TP', False) else '0'
                TP2 = '1' if point.get('TP', False) else ''
                distance = (
                    f"{point['CumulativeDistance'] / 1000:.3f}"
                    if point.get('TP') and pd.notna(point.get('Distance')) and pd.notna(point.get('CumulativeDistance'))
                    else ""
                )

                variant_id = point['VariantCode'] if 'VariantCode' in point else ''

               #lines.append(f"rvpoint|{stop}|{TP1}|{TP2}|{distance}|{variant_id}")
                lines.append(f"rvpoint|{stop}|{TP1}|{TP2}|{variant_id}")

    with open(f'{subdir}/route_version.txt', 'w') as f:
        f.write('\n'.join(lines))



def hastus_rte_distances(variant_links, subdir):

    df = variant_links.copy(deep=True)

    # Keep only one entry per From-To combination per variant
    df = df.groupby(['LineId', 'VariantId', 'FromStopPointId', 'ToStopPointId']).first().reset_index()

    # Convert to feet if necessary
    if imperial:
        df['Distance'] = df['Distance'] * 3.28084

    # Handle direction
    df['DirCode'] = df['Direction'].map({'inbound': '4', 'outbound': '5'}).fillna('')

    # Format Distance column: return "" if NaN, otherwise rounded string
    df['FormattedDistance'] = df['Distance'].apply(lambda x: "" if pd.isna(x) else f"{x:.1f}")

    # Format final HASTUS line
    df['Text'] = (
        'variant_itinerary|' +
        df['FromStopPointId'].str[4:] + '|' +
        df['ToStopPointId'].str[4:] + '|' +
        df['FormattedDistance'] + '|' +
        rte_version + '|' +
        df['LineName'] + '|' +
        df['VariantCode'].astype(str) + '|' +
        df['DirCode'] + '|1|1'
    )

    # Write output file
    df['Text'].to_csv(f'{subdir}/route_itinerary.txt', index=False, header=False)


def format_time(td):
    """Converts timedelta to HH:MM format, correctly handling times beyond 24 hours."""
    total_minutes = int(td.total_seconds() // 60)
    hours = (total_minutes // 60)  # Keep full hours beyond 24
    minutes = total_minutes % 60
    return f"{hours}:{minutes:02d}"

def round_to_quarter_hour(td, round_up=False):
    total_minutes = int(td.total_seconds() // 60)
    if round_up:
        rounded_minutes = ((total_minutes + 14) // 15) * 15
    else:
        rounded_minutes = (total_minutes // 15) * 15
    return timedelta(minutes=rounded_minutes)


def hastus_rt_version(trip_stops, trip_subsections, stops, subdir):

    trip_stops = trip_stops.copy(deep=True)
    trip_subsections = trip_subsections.copy(deep=True)
    stops = stops.copy(deep=True)

    trip_stops = trip_stops[['DataId', 'VehicleJourneyCode', 'StopPointId', 'WaitTime']]
    trip_subsections['DepartureTime'] = pd.to_timedelta(trip_subsections['DepartureTime'].astype(str))

    # Sort for group processing
    trip_subsections = trip_subsections.sort_values(by=['DayType', 'LineName', 'BaseJourneyPatternSubSectionId', 'DepartureTime'])
    trip_subsections = add_places(trip_subsections, stops)


    # Generate runtime periods for each (DataId, LineName, BaseJourneyPatternSubSectionId, DayType)
    period_records = []
    for (line, subsection, day), group in trip_subsections.groupby(
        ['LineName', 'BaseJourneyPatternSubSectionId', 'DayType']):

        group = group.sort_values(by='DepartureTime')
        period_start = timedelta(hours=0)
        period_end = timedelta(hours=36)
        trip_times = list(group['DepartureTime'])

        # Step 1: Compute all start times first
        start_times = []

        prev_start = None
        for i, trip_time in enumerate(trip_times):
            if i == 0:
                start = period_start
            else:
                rounded_start = round_to_quarter_hour(trip_time, round_up=False)
                start = max(rounded_start, prev_start + timedelta(minutes=1))

            start_times.append(start)
            prev_start = trip_time

        # Step 2: Compute corresponding end times
        periods = []
        for i, start in enumerate(start_times):
            if i < len(start_times) - 1:
                end = start_times[i + 1] - timedelta(minutes=1)
            else:
                end = period_end  # Last period ends at 36:00
            periods.append([start, end])

        for (start, end), index in zip(periods, group.index):
            period_records.append({
                "LineName": line,
                "VehicleJourneyCode": group.loc[index, 'VehicleJourneyCode'],
                "DepartureTime": group.loc[index, 'DepartureTime'],
                "DataId": group.loc[index, 'DataId'],
                "BaseJourneyPatternSubSectionId": subsection,
                "DayType": day,
                "PeriodStart": start,
                "PeriodEnd": end,
                "FromStopPointId": group.loc[index, 'FromStopPointId'],
                "ToStopPointId": group.loc[index, 'ToStopPointId'],
                "FromPlace": group.loc[index, 'FromPlace'],
                "ToPlace": group.loc[index, 'ToPlace'],
                "RunTimeSec": group.loc[index, 'RunTimeSec']
            })

    # Create a master period table
    all_periods_df = pd.DataFrame(period_records)

    # Find all VariantCodes associated with each (LineName, BaseJourneyPatternSubSectionId, DayType)
    variant_subsection_map = trip_subsections[
        ['LineName', 'VariantCode', 'BaseJourneyPatternSubSectionId', 'DayType']
    ].drop_duplicates()

    # Join to expand runtime periods to all variants that use the same subsection
    expanded = variant_subsection_map.merge(
        all_periods_df,
        on=['LineName', 'BaseJourneyPatternSubSectionId', 'DayType'],
        how='left'
    )

    # Sort final output as required
    expanded = expanded.sort_values(['DayType', 'VariantCode', 'BaseJourneyPatternSubSectionId', 'PeriodStart'])
    load_from = expanded[['DataId', 'LineName', 'DayType', 'VariantCode', 'VehicleJourneyCode', 'DepartureTime', 'FromStopPointId', 'FromPlace',  'PeriodStart', 'PeriodEnd']]
    load_to = expanded[['DataId', 'LineName', 'DayType', 'VariantCode', 'VehicleJourneyCode', 'DepartureTime',  'ToStopPointId', 'ToPlace',  'PeriodStart', 'PeriodEnd']]
    load_from = load_from.rename(columns={'FromPlace': 'Place', 'FromStopPointId': 'StopPointId'})
    load_to = load_to.rename(columns={'ToPlace': 'Place', 'ToStopPointId': 'StopPointId'})
    load = pd.concat([load_from, load_to], axis=0)
    load = load.drop_duplicates(subset=['DataId', 'VehicleJourneyCode', 'Place', 'PeriodStart', 'PeriodEnd'])
    load = load.merge(trip_stops, on=['DataId', 'VehicleJourneyCode', 'StopPointId'], how='left')
    load = load.sort_values(['DayType', 'VariantCode', 'VehicleJourneyCode', 'Place', 'PeriodStart', 'PeriodEnd'])
    load = load.drop_duplicates(subset = ['DayType', 'VariantCode', 'Place', 'PeriodStart'])

    # Convert period times
    expanded['PeriodStart'] = expanded['PeriodStart'].apply(format_time)
    expanded['PeriodEnd'] = expanded['PeriodEnd'].apply(format_time)
    expanded['RunTimeMin'] = expanded['RunTimeSec'].astype(int) // 60

    load['PeriodStart'] = load['PeriodStart'].apply(format_time)
    load['PeriodEnd'] = load['PeriodEnd'].apply(format_time)
    load['WaitTimeMin'] = load['WaitTime'].astype(int) // 60

    # Write to file with runtime_version line per DayType
    with open(f'{subdir}/runtime_version.txt', 'w') as f:
        for day_type, group in expanded.groupby("DayType"):
            day_code = day_type_code[day_type]

            f.write(f"runtime_version|{runtime_version}_{day_code}\n")
            for _, row in group.iterrows():
                f.write(
                    f"runtime_period|{row['LineName']}|{row['VariantCode']}|{row['FromPlace']}|"
                    f"{row['ToPlace']}|{row['RunTimeMin']}|{row['PeriodStart']}|{row['PeriodEnd']}\n"
                )

            load_subset = load.loc[(load['DayType'] == day_type)]

            for _, load_row in load_subset.iterrows():
                f.write(
                    f"load_time|{load_row['Place']}|{load_row['PeriodStart']}|{load_row['PeriodEnd']}|"
                    f"{load_row['WaitTimeMin']}|{load_row['LineName']}|{load_row['VariantCode']}\n"
                )



def hastus_trips(trips, trip_stops, stops, subdir):
    trips = add_places(trips, stops)
    trip_stops = trip_stops.merge(stops, on=['StopPointId'], how='left')
    trip_number = 0
    with open(f'{subdir}/trips.txt', 'w') as f:
        for day_type, group in trips.groupby("DayType"):
            group = group.sort_values("DepartureTime").reset_index(drop=True)
            day_code = day_type_code[day_type]
            sched_code = sched_type_code[day_type]
            vsc_type = service_type_nickname[day_type]

            # Write vehicle_schedule header
            f.write(f"vehicle_schedule|{vsc_name}_{vsc_type}|{day_type} Import from TXC|0|{sched_code}|{rte_version}|{runtime_version}_{day_code}\n")

            for i, row in group.iterrows():
                try:
                    trip_id = row["VehicleJourneyCode"]
                    trip_number += 1
                    route_id = row["LineName"]
                    variant_code = row["VariantCode"]
                    direction = row["HASTUSDirection"]
                    from_place = row["FromPlace"]
                    to_place = row["ToPlace"]
                    departure = row["DepartureTime"][:5]
                    arrival = row["ArrivalTime"][:5]
                    operating_days = '|'.join(str(x) for x in row['OperatingDays'])
                    data_id = row["DataId"]

                    # Write trip header
                    f.write(f"trip|{trip_number}|{route_id}|{trip_number}|{variant_code}|0|{direction}|{from_place}|{to_place}|{departure}|{arrival}|{operating_days}\n")

                    # Get stop rows for this trip with TP == True
                    trip_stop_subset = trip_stops[
                        (trip_stops["DataId"] == data_id) &
                        (trip_stops["VehicleJourneyCode"] == trip_id) &
                        (trip_stops["TP"] == True)
                    ]

                    # Write stop lines
                    for _, stop_row in trip_stop_subset.iterrows():
                        run_time_sec = stop_row.get("CumulativeRunTimeSecs", 0)
                        wait_time_sec = stop_row.get("WaitTime", 0) or 0

                        base_time = compute_arrival_time(row["DepartureTime"], run_time_sec)
                        base_time_str = base_time[:5]

                        stop_id = stop_row["StopPointId"][4:]
                        place_id = stop_row["Place"]

                        f.write(f"trip_tp|{place_id}|{trip_number}|{base_time_str}|1\n")

                        if wait_time_sec > 0:
                            new_time = compute_arrival_time(row["DepartureTime"], run_time_sec + wait_time_sec)
                            f.write(f"trip_tp|{place_id}|{trip_number}|{new_time[:5]}|1\n")

                except Exception as e:
                    print(f"⚠️ Error writing trip row {i}: {e}")



def hastus_locations(stops, subdir):

    stops = stops.copy(deep=True)
    stops = stops.fillna('')

    # Write to file
    with open(f'{subdir}/locations.txt', 'w') as f:
        for _, row in stops.iterrows():
            place = row['Place']
            place_name = row['LocalityName'] + ", " + row['CommonName']
            if place:
                f.write(f"place|{place}|{place_name}\n")

        for _, row in stops.iterrows():
            stop_id = row['StopPointId'][4:]
            stop_name = row['LocalityName'] + ", " + row['CommonName']
            place = row['Place']
            # round latitude to 6 decimal places
            latitude = round(row['Latitude'], 6)
            #latitude = row['Latitude']
            longitude = round(row['Longitude'], 6)
            #longitude = row['Longitude']

            stop_line = f"stop|{stop_id}|{stop_name}|{place}"
            stop_loc_line = f"stop_location|{stop_id}|{stop_name}|{latitude}|{longitude}|1"
            #stop_loc_line = f"stop_location|{stop_id}|{stop_name}|1"
            f.write(stop_line + "\n")
            f.write(stop_loc_line + "\n")


    # with open(f'output/hastus_files/{region}/places.txt', 'w') as f:
    #     # if 'Place' is not null write place line
    #     for _, row in stops.iterrows():
    #         place = row['Place']
    #         place_name = row['LocalityName'] + ", " + row['CommonName']
    #         if place:
    #             f.write(f"place|{place}|{place_name}\n")

from helper.utils import get_output_dir  # or wherever you place it

def create_outputs(transformed_tables, output_dir=None):
    if output_dir is None:
        output_dir = get_output_dir()

    if os.environ.get("AWS_EXECUTION_ENV", "").startswith("AWS_Lambda"):
        subdir = output_dir
    else:
        subdir = os.path.join(output_dir, region, 'hastus_files')

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(subdir, exist_ok=True)

    hastus_rte_version(transformed_tables['VariantPoints'], subdir)
    hastus_rte_distances(transformed_tables['VariantLinks'], subdir)
    hastus_rt_version(transformed_tables['TripStops'], transformed_tables['TripSubSections'], transformed_tables['Stops'], subdir)
    hastus_trips(transformed_tables['Trips'], transformed_tables['TripStops'], transformed_tables['Stops'], subdir)
    hastus_locations(transformed_tables['Stops'], subdir)
    create_link_outputs(transformed_tables['VariantLinks'], transformed_tables['VariantPoints'], output_dir)
