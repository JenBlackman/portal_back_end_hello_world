import pandas as pd
from app.helper.parameters import *
from app.helper.functions import *
from itertools import combinations
import string
import numpy as np


def find_minimal_subsection_set(trip_patterns: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:

    # Remove duplicates at the timing link level (section ID no longer needed)
    grouped_stops = trip_patterns.drop_duplicates(
        subset=["DataId", "JourneyPatternId", "JourneyPatternSectionId", "JourneyPatternSubSectionId",
                "RouteLinkPosition"]
    )

    # Order for consistent stop sequencing and aggregate stop sequences
    grouped_stops = (
        grouped_stops.sort_values([
            "LineName", "DataId", "JourneyPatternId",
            "JourneyPatternSubSectionPosition", "JourneyPatternTimingLinkPositionInJourneyPattern"
        ])
        .groupby(["LineName", "DataId", "JourneyPatternId", "JourneyPatternSubSectionId", "JourneyPatternSubSectionPosition"], as_index=False)
        .agg({"FromStopPointId": list, "ToStopPointId": "last"})
    )

    # Build full stop sequence
    grouped_stops["Stops"] = grouped_stops["FromStopPointId"] + grouped_stops["ToStopPointId"].apply(lambda x: [x])
    grouped_stops = grouped_stops.drop(columns=["FromStopPointId", "ToStopPointId"])

    # Compare stop sequences to find duplicates within LineName
    contained_map = {}
    all_subsections = set(map(tuple, grouped_stops[[
        "LineName", "DataId", "JourneyPatternId",
        "JourneyPatternSubSectionId", "JourneyPatternSubSectionPosition"
    ]].values))
    covered_subsections = set()
    grouped_stops_list = list(grouped_stops.itertuples(index=False))

    for row1, row2 in combinations(grouped_stops_list, 2):
        if row1.LineName != row2.LineName:
            continue
        if row1.Stops == row2.Stops:
            key1 = (row1.LineName, row1.DataId, row1.JourneyPatternId,
                    row1.JourneyPatternSubSectionId, row1.JourneyPatternSubSectionPosition)
            key2 = (row2.LineName, row2.DataId, row2.JourneyPatternId,
                    row2.JourneyPatternSubSectionId, row2.JourneyPatternSubSectionPosition)
            contained_map[key1] = key2
            covered_subsections.add(key1)

    # Determine the minimal (non-duplicated) set
    minimal_set = all_subsections - covered_subsections

    # Ensure all mappings resolve to their final base subsection
    def resolve_mapping(subsection):
        visited = set()
        while subsection in contained_map:
            if subsection in visited:
                break
            visited.add(subsection)
            subsection = contained_map[subsection]
        return subsection

    # Generate final mapping table
    mapping_entries = [
        (*key, resolve_mapping(key)[3])  # Keep existing key, remap subsection ID
        for key in all_subsections
    ]

    subsection_mapping = pd.DataFrame(mapping_entries, columns=[
        "LineName", "DataId", "JourneyPatternId",
        "JourneyPatternSubSectionId", "JourneyPatternSubSectionPosition", "BaseJourneyPatternSubSectionId"
    ]).sort_values([
        "LineName", "DataId", "JourneyPatternId", "JourneyPatternSubSectionPosition"
    ])

    # Filter to only rows part of the minimal set
    minimal_route_links = trip_patterns.merge(
        pd.DataFrame(list(minimal_set), columns=[
            "LineName", "DataId", "JourneyPatternId",
            "JourneyPatternSubSectionId", "JourneyPatternSubSectionPosition"
        ]),
        on=["LineName", "DataId", "JourneyPatternId", "JourneyPatternSubSectionId", "JourneyPatternSubSectionPosition"],
        how="inner"
    )

    return minimal_route_links, subsection_mapping


def find_minimal_variant_set_by_subsections(subsection_mapping):

    # Step 1: Construct ordered sequences of JourneyPatternSubSections for each JourneyPattern
    grouped = (
        subsection_mapping.sort_values([
            "LineName", "DataId", "JourneyPatternId", "JourneyPatternSubSectionPosition"
        ])
        .groupby(["LineName", "DataId", "JourneyPatternId"], group_keys=False)["BaseJourneyPatternSubSectionId"]
        .apply(list)
        .reset_index(name="SubSectionSequence")
    )

    variant_map = {}
    all_variants = set(grouped[["LineName", "DataId", "JourneyPatternId"]].itertuples(index=False, name=None))
    covered_variants = set()
    grouped_list = list(grouped.itertuples(index=False))

    for row1, row2 in combinations(grouped_list, 2):
        if row1.LineName != row2.LineName:
            continue  # Only compare within the same LineName

        seq1, seq2 = row1.SubSectionSequence, row2.SubSectionSequence
        key1 = (row1.LineName, row1.DataId, row1.JourneyPatternId)
        key2 = (row2.LineName, row2.DataId, row2.JourneyPatternId)

        if len(seq1) <= len(seq2) and any(seq1 == seq2[k:k+len(seq1)] for k in range(len(seq2) - len(seq1) + 1)):
            variant_map[key1] = key2
            covered_variants.add(key1)
        elif len(seq2) <= len(seq1) and any(seq2 == seq1[k:k+len(seq2)] for k in range(len(seq1) - len(seq2) + 1)):
            variant_map[key2] = key1
            covered_variants.add(key2)

    def resolve_mapping(variant_key):
        visited = set()
        while variant_key in variant_map:
            if variant_key in visited:
                break
            visited.add(variant_key)
            variant_key = variant_map[variant_key]
        return variant_key

    minimal_set = all_variants - covered_variants

    # Step 2: Create mapping DataFrame
    mapping_entries = [
        (
            line, data_id, journey_id,
            resolve_mapping((line, data_id, journey_id))[2],  # BaseJourneyPatternId
            resolve_mapping((line, data_id, journey_id))[1],  # BaseJourneyPatternDataId
            resolve_mapping((line, data_id, journey_id))[0],  # BaseLineName
        )
        for line, data_id, journey_id in all_variants
    ]

    variant_mapping_df = pd.DataFrame(
        mapping_entries,
        columns=["LineName", "DataId", "JourneyPatternId", "BaseJourneyPatternId", "BaseJourneyPatternDataId", "BaseLineName"]
    )

    # Step 3: Rank and generate VariantCode
    base_sequences = (
        grouped[["LineName", "DataId", "JourneyPatternId", "SubSectionSequence"]]
        .merge(
            variant_mapping_df,
            on=["LineName", "DataId", "JourneyPatternId"],
            how="inner"
        )
        .drop_duplicates(subset=["BaseLineName", "BaseJourneyPatternDataId", "BaseJourneyPatternId"])
    )
    base_sequences["SequenceLength"] = base_sequences["SubSectionSequence"].apply(len)

    base_sequences["VariantRank"] = (
        base_sequences
        .groupby("BaseLineName")["SequenceLength"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    base_sequences["VariantCode"] = base_sequences["BaseLineName"] + "-" + base_sequences["VariantRank"].astype(str)

    # Merge VariantCode into variant_mapping_df
    variant_mapping_df = variant_mapping_df.merge(
        base_sequences[[
            "BaseLineName", "BaseJourneyPatternDataId", "BaseJourneyPatternId", "VariantCode"
        ]],
        on=["BaseLineName", "BaseJourneyPatternDataId", "BaseJourneyPatternId"],
        how="left"
    )

    # Step 4: Filter trip_patterns to include only minimal variants
    minimal_variants = subsection_mapping[
        subsection_mapping.set_index(["LineName", "DataId", "JourneyPatternId"]).index.isin(minimal_set)
    ].reset_index(drop=True)

    return minimal_variants, variant_mapping_df


def import_stops(file_path):

    # import stop points
    stops = pd.read_csv(file_path, low_memory=False)
    stops = stops[['ATCOCode', 'NaptanCode', 'CommonName', 'LocalityName', 'Latitude', 'Longitude']]
    stops.rename(columns={'ATCOCode': 'StopPointId'}, inplace=True)

    return stops



def parse_runtime(runtime_str):

    if pd.isna(runtime_str) or not isinstance(runtime_str, str):
        return 0  # If empty or invalid, return 0 seconds

    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(runtime_str)

    if not match:
        return 0  # Return 0 if the format is unrecognized

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    return hours * 3600 + minutes * 60 + seconds  # Convert to total seconds


def create_journey_pattern_table(journey_patterns, journey_pattern_sections, journey_pattern_timing_links, lines):

    journey_patterns = journey_patterns.copy(deep=True)
    journey_patterns = journey_patterns.drop_duplicates().reset_index(drop=True)
    journey_pattern_sections = journey_pattern_sections.copy(deep=True)
    journey_pattern_sections = journey_pattern_sections.drop_duplicates().reset_index(drop=True)
    journey_pattern_timing_links = journey_pattern_timing_links.copy(deep=True)
    journey_pattern_timing_links = journey_pattern_timing_links.drop_duplicates().reset_index(drop=True)

    journey_patterns = journey_patterns.merge(lines, on=['DataId', 'JourneyPatternId', 'ServiceCode'], how='left')

    trip_pattern_links = journey_pattern_timing_links.merge(journey_pattern_sections, on = ['DataId', 'JourneyPatternSectionId'], how='left')
    trip_pattern_links = trip_pattern_links.merge(journey_patterns, on=['DataId', 'JourneyPatternId'], how='left')


    return trip_pattern_links

def add_days_of_week(vehicle_journeys, services):

    vehicle_journeys = vehicle_journeys.copy(deep=True)
    services = services.copy(deep=True)

    # Fill missing info from services
    services.rename(columns = {'DaysOfWeek': 'ServiceDaysOfWeek', 'BankHolidayNonOperation': 'ServiceBankHolidayNonOperation', 'OperatorId': 'ServiceOperatorId'}, inplace=True)

    vehicle_journeys = vehicle_journeys.merge(services, on=['DataId', 'ServiceCode'],how='left')
    vehicle_journeys['DaysOfWeek'] = vehicle_journeys['DaysOfWeek'].fillna(vehicle_journeys['ServiceDaysOfWeek'])
    vehicle_journeys['BankHolidayNonOperation'] = vehicle_journeys['BankHolidayNonOperation'].fillna(vehicle_journeys['ServiceBankHolidayNonOperation'])
    vehicle_journeys['OperatorId'] = vehicle_journeys['OperatorId'].fillna(vehicle_journeys['ServiceOperatorId'])

    vehicle_journeys = vehicle_journeys.drop(columns=['ServiceCode', 'ServiceDaysOfWeek', 'ServiceBankHolidayNonOperation', 'ServiceOperatorId'])

    return vehicle_journeys


def links_to_points(table, type):

    table = table.copy(deep = 'True')
    table['FromWaitTime'] = table['FromWaitTime'].apply(parse_runtime)
    table['ToWaitTime'] = table['ToWaitTime'].apply(parse_runtime)

    if type == 'trips':
        table = table.sort_values(['DataId', 'LineId', 'VehicleJourneyCode', 'RouteSectionPosition', 'RouteLinkPosition']).reset_index(drop=True)
        table['FirstInRoute'] = (table['VehicleJourneyCode'] != table['VehicleJourneyCode'].shift(1)) | (
                    table['LineId'] != table['LineId'].shift(1)) | (
                    table['DataId'] != table['DataId'].shift(1))

        table['LastInRoute'] = (table['VehicleJourneyCode'] != table['VehicleJourneyCode'].shift(-1)) | (
                    table['LineId'] != table['LineId'].shift(-1)) | (
                    table['DataId'] != table['DataId'].shift(-1))

        table['FirstInSubSection'] = (table['JourneyPatternSubSectionId'] != table['JourneyPatternSubSectionId'].shift(1)) | (
                    table['LineId'] != table['LineId'].shift(1)) | (
                    table['DataId'] != table['DataId'].shift(1))

        table['LastInSubSection'] = (table['JourneyPatternSubSectionId'] != table['JourneyPatternSubSectionId'].shift(-1)) | (
                    table['LineId'] != table['LineId'].shift(-1)) | (
                    table['DataId'] != table['DataId'].shift(-1))

        # handle first stop in variant
        first_stop = table[table['FirstInRoute']].copy(deep=True)
        first_stop['FirstInSubSection'] = True
        first_stop['JourneyPatternTimingLinkPosition'] = 0
        first_stop['RunTime'] = 'PT0H0M00S'
        first_stop['RunTimeSec'] = 0
        first_stop['Distance'] = 0
        first_stop['StopPointId'] = first_stop['FromStopPointId']
        first_stop['TP'] = first_stop['FromTP']

    else:
        table = table.sort_values(['DataId', 'LineId', 'BaseJourneyPatternId', 'JourneyPatternSectionPosition', 'JourneyPatternTimingLinkPosition']).copy(deep=True).reset_index(drop=True)
        table['FirstInRoute'] = (table['VariantCode'] != table['VariantCode'].shift(1)) | (
                table['LineName'] != table['LineName'].shift(1))

        table['LastInRoute'] = (table['VariantId'] != table['VariantId'].shift(-1)) | (
            table['LineName'] != table['LineName'].shift(-1))

        table['FirstInSubSection'] = (table['JourneyPatternSubSectionId'] != table['JourneyPatternSubSectionId'].shift(1)) | (
             table['LineName'] != table['LineName'].shift(1))

        table['LastInSubSection'] = (table['JourneyPatternSubSectionId'] != table['JourneyPatternSubSectionId'].shift(-1)) | (
            table['LineName'] != table['LineName'].shift(-1))



        # handle first stop in variant
        first_stop = table[table['FirstInRoute']].copy(deep=True)
        first_stop['FirstInSubSection'] = True
        first_stop['RouteLinkPosition'] = 0
        first_stop['RunTime'] = 'PT0H0M00S'
        first_stop['RunTimeSec'] = 0
        first_stop['StopPointId'] = first_stop['FromStopPointId']
        first_stop['TP'] = first_stop['FromTP']
        first_stop['JourneyPatternTimingLinkPositionInJourneyPattern'] = 0

        has_missing_distance = (
            table.groupby(['DataId', 'LineId', 'BaseJourneyPatternId'])['Distance']
            .apply(lambda x: x.isna().any())
            .reset_index(name='HasMissing')
        )

        first_stop = first_stop.merge(
            has_missing_distance,
            on=['DataId', 'LineId', 'BaseJourneyPatternId'],
            how='left'
        )

        first_stop['Distance'] = first_stop['HasMissing'].map(lambda x: float('nan') if x else 0.0)
        first_stop.drop(columns='HasMissing', inplace=True)

    # handle remaining stops
    table.loc[table['FirstInRoute'] == True, 'FirstInSubSection'] = False
    table['FirstInRoute'] = False
    table['StopPointId'] = table['ToStopPointId']
    table['TP'] = table['ToTP']

    # concatenate first and remaining stops
    table = pd.concat([first_stop, table])

    if type == 'trips':
        table = table.sort_values(['DataId', 'LineId', 'VehicleJourneyCode', 'RouteSectionPosition', 'RouteLinkPosition']).reset_index(drop=True)
        table['WaitTime'] = (
                table.groupby(['DataId', 'LineId', 'VehicleJourneyCode'])['FromWaitTime'].shift(-1).fillna(0)
                + table['ToWaitTime'].fillna(0)
        )
    else:
        table = table.sort_values(['DataId', 'LineId', 'VariantCode', 'JourneyPatternSectionPosition', 'JourneyPatternTimingLinkPosition']).reset_index(drop=True)

        # Apply condition: same stop as next row → add ToWaitTime + next FromWaitTime
        table['WaitTime'] = (
                table.groupby(['DataId', 'LineId', 'VariantCode'])['FromWaitTime'].shift(-1).fillna(0)
                + table['ToWaitTime'].fillna(0)
        )

    # convert to integer
    table['WaitTime'] = table['WaitTime'].astype(int)

    return table


def add_day_type(table):
    table = table.copy(deep=True)
    dow = table['DaysOfWeek'].fillna('')

    # Boolean checks
    has_weekend = dow.str.contains('Saturday|Sunday', na=False)
    has_weekday = dow.str.contains('Monday|Tuesday|Wednesday|Thursday|Friday', na=False)
    is_bank_or_holiday = dow.str.contains('BankHoliday|HolidaysOnly', na=False)

    # Only Sat+Sun (and nothing else)
    is_weekend_only = (
        dow.str.contains('Saturday', na=False) &
        dow.str.contains('Sunday', na=False) &
        ~has_weekday &
        ~is_bank_or_holiday
    )

    is_sunday_only = (
        dow.str.contains('Sunday', na=False) &
        ~dow.str.contains('Saturday|Monday|Tuesday|Wednesday|Thursday|Friday|BankHoliday|HolidaysOnly', na=False)
    )

    is_saturday_only = (
        dow.str.contains('Saturday', na=False) &
        ~dow.str.contains('Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|BankHoliday|HolidaysOnly', na=False)
    )

    # Define in priority order
    conditions = [
        is_bank_or_holiday,
        has_weekend & has_weekday,
        is_weekend_only,
        is_sunday_only,
        is_saturday_only
    ]
    choices = [
        'Holiday',
        'AllWeek',
        'Weekend',
        'Sunday',
        'Saturday'
    ]

    table['DayType'] = np.select(conditions, choices, default='Weekday')
    return table


def operates_on_day(day, day_string):
    return int(day in day_string)


def add_operating_days(table):
    table = table.copy(deep=True)
    day_strs = table["DaysOfWeek"].fillna('')

    def compute_operating_days(row):
        if row.get("DayType") in ["AllWeek", "Holiday"]:
            return [1] * 7
        return [operates_on_day(day, row["DaysOfWeek"]) for day in days]

    table["OperatingDays"] = table.apply(compute_operating_days, axis=1)
    return table

def add_trip_pattern_info(vehicle_journey_links: pd.DataFrame, trip_patterns: pd.DataFrame) -> pd.DataFrame:
    """Merge trip pattern information into vehicle journey links, with per-row fallback if JourneyPatternTimingLinkId is missing."""

    # Identify shared columns excluding merge keys
    base_keys = ['DataId', 'LineId', 'JourneyPatternId']
    full_keys = base_keys + ['JourneyPatternTimingLinkId']
    shared_columns = set(vehicle_journey_links.columns) & set(trip_patterns.columns) - set(full_keys)

    # Split vehicle_journey_links based on presence of JourneyPatternTimingLinkId
    with_timing = vehicle_journey_links[vehicle_journey_links['JourneyPatternTimingLinkId'].notna()].copy()
    without_timing = vehicle_journey_links[vehicle_journey_links['JourneyPatternTimingLinkId'].isna()].copy()

    # Merge each subset accordingly
    merged_with_timing = with_timing.merge(
        trip_patterns,
        on=full_keys,
        how='left',
        suffixes=('_journey', '_pattern')
    )

    merged_without_timing = without_timing.merge(
        trip_patterns,
        on=base_keys,
        how='left',
        suffixes=('_journey', '_pattern')
    )

    # Concatenate both parts back together
    merged = pd.concat([merged_with_timing, merged_without_timing], ignore_index=True)

    # Fill in values from pattern where journey values are missing
    for col in shared_columns:
        journey_col = f"{col}_journey"
        pattern_col = f"{col}_pattern"
        if journey_col in merged.columns and pattern_col in merged.columns:
            merged[col] = merged[journey_col].fillna(merged[pattern_col])

    # Drop the suffixed columns
    merged.drop(
        [col for col in merged.columns if col.endswith('_journey') or col.endswith('_pattern')],
        axis=1,
        inplace=True
    )

    return merged

def map_column_names(txc_tables, column_mapping):

    return {table_name: df.rename(columns=column_mapping) for table_name, df in txc_tables.items()}


def manage_distances(txc_tables):
    for key, df in txc_tables.items():
        if 'Distance' in df.columns:  # Check if 'Distance' column exists
            df['Distance'] = df['Distance'].replace("None", None).astype(float)

    return txc_tables


def add_subsections(trip_patterns):

    # Step 1: Recalculate position within JourneyPattern
    trip_patterns = trip_patterns.sort_values(
        ['DataId', 'JourneyPatternId', 'JourneyPatternSectionPosition', 'JourneyPatternTimingLinkPosition']
    ).copy()

    trip_patterns['JourneyPatternTimingLinkPositionInJourneyPattern'] = (
        trip_patterns.groupby(['DataId', 'JourneyPatternId']).cumcount() + 1
    )

    # Step 2: Create subsection IDs based on FromTP
    trip_patterns['JourneyPatternSubSectionId'] = None
    trip_patterns['JourneyPatternSubSectionPosition'] = None

    for (data_id, journey_pattern_id), group in trip_patterns.groupby(['DataId', 'JourneyPatternId']):
        group = group.sort_values(by='JourneyPatternTimingLinkPositionInJourneyPattern')
        subsection_id = 1
        subsection_ids, subsection_positions = [], []

        for _, row in group.iterrows():
            if row['JourneyPatternTimingLinkPositionInJourneyPattern'] == 1:
                subsection_id = 1
            elif row['FromTP']:
                subsection_id += 1

            sub_id = f"{data_id}_{journey_pattern_id}_{subsection_id}"
            subsection_ids.append(sub_id)
            subsection_positions.append(subsection_id)

        trip_patterns.loc[group.index, 'JourneyPatternSubSectionId'] = subsection_ids
        trip_patterns.loc[group.index, 'JourneyPatternSubSectionPosition'] = subsection_positions

    return trip_patterns


def create_routes_table(routes, route_sections, route_links, lines, stops):
    # Merge RouteSections with RouteLinks
    #route_links = routes.merge(route_sections, on=['RouteId', 'RouteSectionId'], how='left')
    route_sections = route_sections.copy(deep=True)
    route_sections = route_sections.drop_duplicates().reset_index(drop=True)
    route_links = route_links.copy(deep=True)
    route_links = route_links.drop_duplicates().reset_index(drop=True)

    # Merge RouteLinks with Routes
    route_links = route_sections.merge(route_links, on=['DataId', 'RouteSectionId'], how='left')
    route_links = route_links.merge(routes, on=['DataId', 'VariantId'], how='left')
    # Merge RouteLinks with Lines
    #route_links = route_links.merge(lines, on='RouteId', how='left')

    # Merge RouteLinks with Stops
    #route_links = route_links.merge(stops, on='StopPointId', how='left')

    # Drop unnecessary columns
    #route_links.drop(columns=['RouteSectionId', 'RouteSectionPosition', 'RouteLinkId', 'StopPointId'], inplace=True)

    return route_links


def create_trip_patterns(journey_pattern_table, route_table):

    trip_patterns = journey_pattern_table.merge(
        route_table,
        left_on=['DataId', 'VariantId', 'RouteLinkId', 'JourneyPatternSectionPosition',
                 'JourneyPatternTimingLinkPosition'],
        right_on=['DataId', 'VariantId', 'RouteLinkId', 'RouteSectionPosition', 'RouteLinkPosition'],
        how='left',
        suffixes=('_trip', '_path')
    )

    # Identify shared columns (excluding merge keys)
    merge_keys = ['DataId', 'VariantId', 'RouteLinkId', 'JourneyPatternSectionPosition',
                  'JourneyPatternTimingLinkPosition']
    shared_columns = set(journey_pattern_table.columns) & set(route_table.columns)
    shared_columns -= set(merge_keys)  # Remove merge keys from shared columns

    # Replace missing values in journey_pattern_table columns with values from route_table
    for col in shared_columns:
        trip_patterns[col] = trip_patterns[f"{col}_trip"].fillna(trip_patterns[f"{col}_path"])

    # Drop redundant columns
    columns_to_drop = [f"{col}_trip" for col in shared_columns] + [f"{col}_path" for col in shared_columns]
    trip_patterns.drop(columns=columns_to_drop, inplace=True)

    return trip_patterns


def add_variant_info(minimal_route_points, variant_info):

    # Identify overlapping columns
    merge_keys = {'DataId', 'LineName', 'VariantId', 'ServiceCode'}
    shared_columns = set(minimal_route_points.columns) & set(variant_info.columns)
    shared_columns -= merge_keys  # Remove merge keys from shared columns

    # Merge DataFrames on common keys, adding suffixes to prevent overwriting
    minimal_route_points = minimal_route_points.merge(
        variant_info,
        on=['DataId', 'LineName', 'VariantId', 'ServiceCode'],
        how='left',
        suffixes=('_route', '_variant')
    )

    # Replace missing values in variant columns with values from route columns
    for col in shared_columns:
        variant_col = f"{col}_variant"
        route_col = f"{col}_route"

        if variant_col in minimal_route_points.columns and route_col in minimal_route_points.columns:
            minimal_route_points[col] = minimal_route_points[variant_col].fillna(minimal_route_points[route_col])

    # Drop redundant columns safely (only if they exist)
    columns_to_drop = [
        col for col in minimal_route_points.columns
        if col.endswith('_variant') or col.endswith('_route')
    ]

    minimal_route_points.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    return minimal_route_points

def boolean_tps(table):
    table['FromTP'] = table['FromTimingStatus'].isin(
        ['PTP', 'principalTimingPoint'])
    table['ToTP'] = table['ToTimingStatus'].isin(
        ['PTP', 'principalTimingPoint'])
    return table


def get_variant_info(vehicle_journeys, journey_patterns, lines, services):

    variant_info = vehicle_journeys[['DataId', 'ServiceCode', 'LineId', 'JourneyPatternId']].copy(deep=True)
    variant_info = variant_info.drop_duplicates(subset=['DataId', 'ServiceCode', 'LineId', 'JourneyPatternId']).reset_index(drop=True)
    variant_info = variant_info.merge(lines, on=['DataId', 'LineId', 'ServiceCode'], how='left')
    variant_info = variant_info.merge(journey_patterns, on=['DataId', 'ServiceCode', 'JourneyPatternId'], how='left')
    variant_info = variant_info.drop_duplicates(subset = ['DataId', 'VariantId', 'Direction']).reset_index(drop=True)
    variant_info = variant_info.merge(services, on=['DataId', 'ServiceCode'], how='left')

    variant_info['OutboundDescriptionShort'] = variant_info['OutboundOrigin'] + ' - ' + variant_info[
        'OutboundDestination']
    # Fill missing values with the full description
    variant_info['OutboundDescriptionShort'] = variant_info['OutboundDescriptionShort'].fillna(
        variant_info['OutboundDescription'])
    variant_info['InboundDescriptionShort'] = variant_info['InboundOrigin'] + ' - ' + variant_info[
        'InboundDestination']
    # Fill missing values with the full description
    variant_info['InboundDescriptionShort'] = variant_info['InboundDescriptionShort'].fillna(
        variant_info['InboundDescription'])

    return variant_info


def get_lines(vehicle_journeys, lines):

        lines = lines.copy(deep=True)
        pattern_lines = vehicle_journeys[['DataId', 'ServiceCode', 'LineId', 'JourneyPatternId']].copy(deep=True)
        pattern_lines = pattern_lines.drop_duplicates().reset_index(drop=True)

        lines = lines.drop_duplicates(subset=['DataId', 'LineId']).reset_index(drop=True)
        pattern_lines = pattern_lines.merge(lines, on=['DataId', 'ServiceCode', 'LineId'], how='left')

        return pattern_lines

def cumulative_dist(variant_points):
    variant_points = variant_points.copy()

    def reset_cumsum(group):
        reset_group = group['FirstInSubSection'].cumsum()
        group['CumulativeDistance'] = group.groupby(reset_group)['Distance'].cumsum()
        return group

    variant_points = (
        variant_points
        .sort_values(['DataId', 'LineId', 'BaseJourneyPatternId', 'JourneyPatternSectionPosition', 'JourneyPatternTimingLinkPosition'])
        .groupby(['DataId', 'LineName', 'VariantCode'], group_keys=False)
        .apply(reset_cumsum)
        .reset_index(drop=True)
    )

    return variant_points

def create_stop_stop_paths(variant_links, stops):
    """Fill only missing Paths in variant_links using stop coordinates."""

    # Step 1: Prepare stop coordinates
    stop_coords = stops[['StopPointId', 'Latitude', 'Longitude']]

    # Merge 'From' coordinates
    variant_links = variant_links.merge(
        stop_coords.rename(columns={'StopPointId': 'FromStopPointId', 'Latitude': 'FromLat', 'Longitude': 'FromLon'}),
        on='FromStopPointId', how='left'
    )

    # Merge 'To' coordinates
    variant_links = variant_links.merge(
        stop_coords.rename(columns={'StopPointId': 'ToStopPointId', 'Latitude': 'ToLat', 'Longitude': 'ToLon'}),
        on='ToStopPointId', how='left'
    )

    # Step 2: Only create a Path if it is missing
    mask_missing = variant_links['Path'].isna()

    def build_line(row):
        if pd.notna(row['FromLon']) and pd.notna(row['FromLat']) and pd.notna(row['ToLon']) and pd.notna(row['ToLat']):
            return LineString([(row['FromLon'], row['FromLat']), (row['ToLon'], row['ToLat'])])
        else:
            return None

    variant_links.loc[mask_missing, 'Path'] = variant_links.loc[mask_missing].apply(build_line, axis=1)

    return variant_links


def prepare_stops(txc_tables, uk_stops_path='InputFiles/stops/stop-codes.csv', places_path='InputFiles/stops/stops-with-places2.csv'):
    try:
        uk_stops = import_stops(uk_stops_path)
    except Exception as e:
        raise FileNotFoundError(f"Failed to load UK stops from {uk_stops_path}: {e}")

    if 'StopPoints' not in txc_tables or 'StopPointId' not in txc_tables['StopPoints'].columns:
        raise KeyError("TXC tables do not contain 'StopPoints' with a 'StopPointId' column.")

    base_stops = (
        txc_tables['StopPoints'][['StopPointId']]
        .drop_duplicates(subset=['StopPointId'])
        .reset_index(drop=True)
    )

    stops = base_stops.merge(uk_stops, on='StopPointId', how='left')

    use_generated_places = True

    try:
        stops_with_places = pd.read_csv(places_path, low_memory=False)

        if {'StopPointId', 'Place'}.issubset(stops_with_places.columns):
            stops = stops.merge(stops_with_places[['StopPointId', 'Place']], on='StopPointId', how='left')
            use_generated_places = False
        else:
            print(f"Warning: '{places_path}' found but missing required headers. Falling back to generated Place values.")

    except FileNotFoundError:
        print(f"Warning: '{places_path}' not found. Generating Place values from StopPointId.")

    if use_generated_places:
        stops['Place'] = stops['StopPointId'].astype(str).str[-6:]

        place_counts = stops['Place'].value_counts()
        duplicates = place_counts[place_counts > 1].index

        for dup in duplicates:
            dup_rows = stops[stops['Place'] == dup].index.tolist()
            for i, idx in enumerate(dup_rows):
                if i == 0:
                    continue  # Keep original
                if i - 1 >= 26:
                    raise ValueError(f"Too many duplicates for Place '{dup}': cannot generate unique 6-character values.")
                prefix = string.ascii_uppercase[i - 1]
                stops.at[idx, 'Place'] = prefix + dup[1:]

    return stops

def extract_variant_info(txc_tables):
    return get_variant_info(
        txc_tables['VehicleJourneys'],
        txc_tables['JourneyPatterns'],
        txc_tables['Lines'],
        txc_tables['Services']
    )


def prepare_vehicle_journeys(txc_tables):
    vehicle_journeys = enrich_vehicle_journeys(txc_tables['VehicleJourneys'], txc_tables['Services'])
    vehicle_journey_links = txc_tables['VehicleJourneyTimingLinks'].merge(vehicle_journeys, on=['DataId', 'VehicleJourneyCode'], how='left')
    return vehicle_journeys, vehicle_journey_links


def summarise_outputs(trips, trip_stops, stops):
    trip_summary = summarise_trips(trips)
    stops_by_route = summarise_stops_by_route(trip_stops, stops)
    return trip_summary, stops_by_route


def enrich_vehicle_journeys(vehicle_journeys, services):
    vj = add_days_of_week(vehicle_journeys, services)
    vj = add_day_type(vj)
    vj = add_operating_days(vj)
    return vj


def extract_variant_links(trip_patterns):
    return trip_patterns[
        (trip_patterns["JourneyPatternId"] == trip_patterns["BaseJourneyPatternId"]) &
        (trip_patterns["DataId"] == trip_patterns["BaseJourneyPatternDataId"])
    ]


def summarise_trips(trips):
    return trips.groupby(['DataId', 'LineName', 'DayType', 'Direction']).agg(
        Count=('LineName', 'count'),
        Variants=('VariantCode', 'unique')
    ).reset_index()


def summarise_stops_by_route(trip_stops, stops):
    stops_by_route = trip_stops.copy()
    stops_by_route['Terminus'] = stops_by_route['FirstInRoute'] | stops_by_route['FirstInRoute'].shift(-1)

    stops_by_route = (
        stops_by_route[stops_by_route['TP'] == True]
        .groupby('StopPointId')
        .agg(
            Routes=('LineName', lambda x: list(x.unique())),
            TerminusRoutes=('Terminus', lambda t: list(stops_by_route.loc[t.index[t], 'LineName'].unique()))
        )
        .reset_index()
    )

    return stops_by_route.merge(stops, on='StopPointId', how='left')


def create_all_hastus_trip_tables(vehicle_journey_links, trip_patterns):
    trip_links = add_trip_pattern_info(vehicle_journey_links, trip_patterns)

    trip_links['RunTimeSec'] = trip_links['RunTime'].apply(parse_runtime)
    trip_links['HASTUSDirection'] = trip_links['Direction'].map({'inbound': '4', 'outbound': '5'})

    trip_stops = links_to_points(trip_links, 'trips')

    trip_sections = build_trip_sections(trip_stops)
    trip_subsections = build_trip_subsections(trip_links)
    trips = build_trip_headers(trip_sections)

    return trips, trip_sections, trip_subsections, trip_stops


def build_trip_sections(trip_stops):
    trip_stops = trip_stops.copy()
    trip_stops['Distance'] = trip_stops['Distance'].replace("None", None).astype(float)

    trip_sections = trip_stops.groupby(
        ['DataId', 'LineId', 'VariantId', 'VehicleJourneyCode', 'JourneyPatternId','JourneyPatternSectionPosition']).agg(
        LineName=('BaseLineName', 'first'),
        BaseJourneyPatternId=('BaseJourneyPatternId', 'first'),
        VariantCode=('VariantCode', 'first'),
        DepartureTime=('DepartureTime', 'first'),
        Distance=('Distance', handle_distance),
        Direction=('Direction', 'first'),
        HASTUSDirection=('HASTUSDirection', 'first'),
        RunTimeSec=('RunTimeSec', 'sum'),
        Links=('RouteLinkId', list),
        FromStopPointId=('FromStopPointId', 'first'),
        ToStopPointId=('ToStopPointId', 'last'),
        TP=('TP', 'any'),
        DayType=('DayType', 'first'),
        OperatingDays=('OperatingDays', 'first')
    ).reset_index()

    return trip_sections


def build_trip_subsections(trip_links):
    return trip_links.groupby(
        ['DataId', 'LineId', 'VariantId', 'VehicleJourneyCode', 'JourneyPatternId', 'JourneyPatternSubSectionId']).agg(
        LineName=('BaseLineName', 'first'),
        BaseJourneyPatternId=('BaseJourneyPatternId', 'first'),
        BaseJourneyPatternSubSectionId=('BaseJourneyPatternSubSectionId', 'first'),
        DepartureTime=('DepartureTime', 'first'),
        Distance=('Distance', handle_distance),
        Direction=('Direction', 'first'),
        HASTUSDirection=('HASTUSDirection', 'first'),
        RunTimeSec=('RunTimeSec', 'sum'),
        Links=('RouteLinkId', list),
        FromStopPointId=('FromStopPointId', 'first'),
        ToStopPointId=('ToStopPointId', 'last'),
        VariantCode=('VariantCode', 'first'),
        DayType=('DayType', 'first')
    ).reset_index()


def build_trip_headers(trip_sections):
    trips = trip_sections.groupby(
        ['DataId', 'LineId', 'VariantId', 'VehicleJourneyCode', 'JourneyPatternId']).agg(
        LineName=('LineName', 'first'),
        BaseJourneyPatternId=('BaseJourneyPatternId', 'first'),
        DepartureTime=('DepartureTime', 'first'),
        Distance=('Distance', handle_distance),
        Direction=('Direction', 'first'),
        HASTUSDirection=('HASTUSDirection', 'first'),
        RunTimeSec=('RunTimeSec', 'sum'),
        FromStopPointId=('FromStopPointId', 'first'),
        ToStopPointId=('ToStopPointId', 'last'),
        VariantCode=('VariantCode', 'first'),
        DayType=('DayType', 'first'),
        OperatingDays=('OperatingDays', 'first')
    ).reset_index()

    trips['ArrivalTime'] = trips.apply(lambda row: compute_arrival_time(row["DepartureTime"], row["RunTimeSec"]), axis=1)
    return trips


def create_full_trip_patterns(journey_pattern_table, route_table):
    trip_patterns = create_trip_patterns(journey_pattern_table, route_table)
    trip_patterns = boolean_tps(trip_patterns)
    trip_patterns = add_subsections(trip_patterns)
    return trip_patterns


def rationalise_subsections_and_variants(trip_patterns):
    minimal_links, subsection_mapping = find_minimal_subsection_set(trip_patterns)
    minimal_sections, variant_mapping = find_minimal_variant_set_by_subsections(subsection_mapping)
    trip_patterns = trip_patterns.merge(subsection_mapping, on=['DataId', 'JourneyPatternId', 'JourneyPatternSubSectionId', 'JourneyPatternSubSectionPosition', 'LineName'], how='left')
    trip_patterns = trip_patterns.merge(variant_mapping, on=['DataId', 'JourneyPatternId', 'LineName'], how='left')
    return trip_patterns, subsection_mapping, variant_mapping


def generate_variant_geometry(trip_patterns, stops, variant_info):
    variant_links = extract_variant_links(trip_patterns)
    variant_links = create_stop_stop_paths(variant_links, stops)
    variant_points = links_to_points(variant_links, 'variants')
    variant_points = add_variant_info(variant_points, variant_info)
    variant_points = variant_points.merge(stops, on='StopPointId', how='left')
    variant_points = cumulative_dist(variant_points)
    return variant_links, variant_points


def transform_all_txc_tables(txc_tables_static):
    txc_tables = {key: df.copy(deep=True) for key, df in txc_tables_static.items()}
    txc_tables = map_column_names(txc_tables, column_mapping)
    txc_tables = manage_distances(txc_tables)

    stops = prepare_stops(txc_tables)
    variant_info = extract_variant_info(txc_tables)

    route_table = create_routes_table(txc_tables['Routes'], txc_tables['RouteSections'], txc_tables['RouteLinks'], txc_tables['Lines'], stops)
    journey_pattern_lines = get_lines(txc_tables['VehicleJourneys'], txc_tables['Lines'])
    journey_pattern_table = create_journey_pattern_table(txc_tables['JourneyPatterns'], txc_tables['JourneyPatternSections'], txc_tables['JourneyPatternTimingLinks'], journey_pattern_lines)

    trip_patterns = create_full_trip_patterns(journey_pattern_table, route_table)
    trip_patterns, subsection_mapping, variant_mapping = rationalise_subsections_and_variants(trip_patterns)
    variant_links, variant_points = generate_variant_geometry(trip_patterns, stops, variant_info)

    vehicle_journeys, vehicle_journey_links = prepare_vehicle_journeys(txc_tables)
    trips, trip_sections, trip_subsections, trip_stops = create_all_hastus_trip_tables(vehicle_journey_links, trip_patterns)

    trip_summary, stops_by_route = summarise_outputs(trips, trip_stops, stops)

    transformed_tables = {
        'VariantMapping': variant_mapping,
        'VariantLinks': variant_links,
        'VariantPoints': variant_points,
        'Trips': trips,
        'TripSections': trip_sections,
        'TripSubSections': trip_subsections,
        'TripStops': trip_stops,
        'Stops': stops
    }

    return {
        'VariantMapping': variant_mapping,
        'VariantLinks': variant_links,
        'VariantPoints': variant_points,
        'Trips': trips,
        'TripSections': trip_sections,
        'TripSubSections': trip_subsections,
        'TripStops': trip_stops,
        'Stops': stops,
        'TripSummary': trip_summary,
        'StopsByRoute': stops_by_route
    }
