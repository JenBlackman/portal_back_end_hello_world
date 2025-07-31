import os
import pandas as pd
import xml.etree.ElementTree as ET
from app.helper.functions import get_text, get_linestring, extract_days_of_week, extract_raw_days
from app.helper.parameters import NAMESPACES  # Import NAMESPACES from helper.parameters


# Function to parse and create DataFrame from XML files
def process_xml_file(file_path, dataid, filename):
    try:
        # Attempt to parse XML
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"❌ ERROR: Failed to parse {file_path} - {e}")
        return {}  # Return empty dictionary to prevent crashes


    # Function to parse XML sections and ensure Route is included
    def parse_section(tag, mappings, dataid):
        """Parses a given XML section into a list of dictionaries and ensures Route is included."""
        return [
            {'DataId': dataid} | {key: get_text(elem, path) for key, path in mappings.items()}
            for elem in root.findall(f'.//txc:{tag}', NAMESPACES)
        ]

    import_summary = [
        {
            'DataId': dataid,
            'FileName': filename
        }
    ]


    # # Extracting different sections
    # serviced_orgs = parse_section('ServicedOrganisation', {
    #     'OrganisationCode': 'txc:OrganisationCode',
    #     'Name': 'txc:Name',
    #     'StartDate': './/txc:StartDate',
    #     'EndDate': './/txc:EndDate'
    # }, dataid)

    serviced_orgs = []

    for org in root.findall('.//txc:ServicedOrganisation', NAMESPACES):
        org_code = get_text(org, 'txc:OrganisationCode')
        name = get_text(org, 'txc:Name')

        for dr in org.findall('.//txc:WorkingDays/txc:DateRange', NAMESPACES):
            serviced_orgs.append({
                'DataId': dataid,
                'OrganisationCode': org_code,
                'Name': name,
                'StartDate': get_text(dr, 'txc:StartDate'),
                'EndDate': get_text(dr, 'txc:EndDate'),
            })


    stop_points = [{
        'DataId': dataid,
        'StopPointRef': get_text(stop_point, 'txc:StopPointRef'),
        'CommonName': get_text(stop_point, 'txc:CommonName'),
        'Longitude': get_text(stop_point, 'txc:Location/txc:Longitude'),
        'Latitude': get_text(stop_point, 'txc:Location/txc:Latitude')
    }
    for stop_point in root.findall('.//txc:AnnotatedStopPointRef', NAMESPACES)
    ]

    routes = [
        {
            'DataId': dataid,
            'RouteId': route.attrib.get('id'),
            'Description': get_text(route, 'txc:Description'),
        }
        for route in root.findall('.//txc:Route', NAMESPACES)
    ]

    route_links = [
        {
            'DataId': dataid,
            #'FileRoute': route_name,
            'RouteSectionId': section.attrib.get('id'),
            'RouteLinkId': link.attrib.get('id'),
            'RouteLinkPosition': idx + 1,
            'FromStopPointRef': get_text(link, 'txc:From/txc:StopPointRef'),
            'FromWaitTime': get_text(link, 'txc:From/txc:WaitTime'),
            'ToStopPointRef': get_text(link, 'txc:To/txc:StopPointRef'),
            'ToWaitTime': get_text(link, 'txc:To/txc:WaitTime'),
            'Distance': get_text(link, 'txc:Distance'),
            'Direction': get_text(link, 'txc:Direction'),
            'Path': get_linestring(link)
        }
        for section in root.findall('.//txc:RouteSection', NAMESPACES)
        for idx, link in enumerate(section.findall('txc:RouteLink', NAMESPACES))
    ]

    route_sections = [
        {
            'DataId': dataid,
            'RouteId': get_text(route, '@id'),  # Extract RouteId from attribute
            'RouteSectionId': ref.text,  # Each RouteSectionRef on a separate row
            'RouteSectionPosition': idx + 1
        }
        for route in root.findall('.//txc:Route', NAMESPACES)
        for idx, ref in enumerate(route.findall('.//txc:RouteSectionRef', NAMESPACES))
    ]


    journey_pattern_timing_links = [
        {
            'DataId': dataid,
            'JourneyPatternSectionId': jps.attrib.get('id'),
            'JourneyPatternTimingLinkId': link.attrib.get('id'),
            'FromActivity': get_text(link, 'txc:From/txc:Activity'),
            'FromStopPointRef': get_text(link, 'txc:From/txc:StopPointRef'),
            'FromTimingStatus': get_text(link, 'txc:From/txc:TimingStatus'),
            'FromWaitTime': get_text(link, 'txc:From/txc:WaitTime'),
            'ToActivity': get_text(link, 'txc:To/txc:Activity'),
            'ToStopPointRef': get_text(link, 'txc:To/txc:StopPointRef'),
            'ToTimingStatus': get_text(link, 'txc:To/txc:TimingStatus'),
            'ToWaitTime': get_text(link, 'txc:To/txc:WaitTime'),
            'RunTime': get_text(link, 'txc:RunTime'),
            'Distance': get_text(link, 'txc:Distance'),
            'RouteLinkRef': get_text(link, 'txc:RouteLinkRef'),
            'JourneyPatternTimingLinkPosition': idx + 1  # ✅ Assigns sequence number
        }
        for jps in root.findall('.//txc:JourneyPatternSection', NAMESPACES)
        for idx, link in enumerate(jps.findall('txc:JourneyPatternTimingLink', NAMESPACES))
    ]


    operators = parse_section('Operator', {
        'OperatorId': '@id',  # Extract Operator ID as an attribute
        'NationalOperatorCode': 'txc:NationalOperatorCode',
        'OperatorShortName': 'txc:OperatorShortName',
        'LicenceNumber': 'txc:LicenceNumber'
    }, dataid)

    services = [
        {
            'DataId': dataid,
            'ServiceCode': get_text(service, 'txc:ServiceCode'),
            'Mode': get_text(service, 'txc:Mode'),
            'StartDate': get_text(service, 'txc:OperatingPeriod/txc:StartDate'),
            'EndDate': get_text(service, 'txc:OperatingPeriod/txc:EndDate'),
            'DaysOfWeek': extract_days_of_week(service),
            'BankHolidayNonOperation': ', '.join(
                [bh.tag.split('}')[-1] for bh in
                 service.findall('.//txc:BankHolidayOperation/txc:DaysOfNonOperation/*', NAMESPACES)]
            ),
            'BankHolidayOperation': ', '.join(
                [bh.tag.split('}')[-1] for bh in
                 service.findall('.//txc:BankHolidayOperation/txc:DaysOfOperation/*', NAMESPACES)]
            ),
            'RegisteredOperatorRef': get_text(service, 'txc:RegisteredOperatorRef'),
            'StopRequirements': get_text(service, 'txc:StopRequirements/txc:NoNewStopsRequired'),
            'Origin': get_text(service, 'txc:StandardService/txc:Origin'),
            'Destination': get_text(service, 'txc:StandardService/txc:Destination'),
            'Vias': ', '.join(
                [via.text for via in service.findall('.//txc:StandardService/txc:Vias/txc:Via', NAMESPACES)])
        }
        for service in root.findall('.//txc:Service', NAMESPACES)
    ]

    lines = [
        {
            'DataId': dataid,
            'ServiceCode': get_text(service, 'txc:ServiceCode'),
            'LineId': line.attrib.get('id') if line is not None else None,
            'LineName': get_text(line, 'txc:LineName'),
            'OutboundOrigin': get_text(line, 'txc:OutboundDescription/txc:Origin'),
            'OutboundDestination': get_text(line, 'txc:OutboundDescription/txc:Destination'),
            'OutboundDescription': get_text(line, 'txc:OutboundDescription/txc:Description'),
            'InboundOrigin': get_text(line, 'txc:InboundDescription/txc:Origin'),
            'InboundDestination': get_text(line, 'txc:InboundDescription/txc:Destination'),
            'InboundDescription': get_text(line, 'txc:InboundDescription/txc:Description'),
        }
        for service in root.findall('.//txc:Service', NAMESPACES)
        for line in service.findall('.//txc:Line', NAMESPACES) or [None]  # ✅ Handles cases where no <Line> is present
    ]

    journey_patterns = [
        {
            'DataId': dataid,
            'ServiceCode': get_text(service, 'txc:ServiceCode'),
            'JourneyPatternId': jp.attrib.get('id') if jp is not None else None,
            'Direction': get_text(jp, 'txc:Direction'),
            'RouteId': get_text(jp, 'txc:RouteRef'),
            'DestinationDisplay': get_text(jp, 'txc:DestinationDisplay'),
            'OperatorRef': get_text(jp, 'txc:OperatorRef'),
            'BlockNumber': get_text(jp, 'txc:Operational/txc:Block/txc:BlockNumber'),
            'BlockDescription': get_text(jp, 'txc:Operational/txc:Block/txc:Description'),
        }
        for service in root.findall('.//txc:Service', NAMESPACES)
        for jp in service.findall('.//txc:StandardService/txc:JourneyPattern', NAMESPACES)
    ]

    journey_pattern_sections = [
        {
            'DataId': dataid,
            'JourneyPatternId': jp.attrib.get('id') if jp is not None else None,
            'JourneyPatternSectionPosition': idx + 1,
            'JourneyPatternSectionRefs': jps.text if jps is not None else None
        }
        for service in root.findall('.//txc:Service', NAMESPACES)
        for jp in service.findall('.//txc:StandardService/txc:JourneyPattern', NAMESPACES)
        for idx, jps in enumerate(jp.findall('txc:JourneyPatternSectionRefs', NAMESPACES))
    ]

    #
    # vehicle_journeys = [
    #     {
    #         'DataId': dataid,
    #         'TicketMachineJourneyCode': get_text(journey, 'txc:Operational/txc:TicketMachine/txc:JourneyCode'),
    #         'VehicleJourneyCode': get_text(journey, 'txc:VehicleJourneyCode'),
    #         'PrivateCode': get_text(journey, 'txc:PrivateCode'),
    #         'OperatorRef': get_text(journey, 'txc:OperatorRef'),
    #         'ServiceRef': get_text(journey, 'txc:ServiceRef'),
    #         'LineRef': get_text(journey, 'txc:LineRef'),
    #         'DepartureTime': get_text(journey, 'txc:DepartureTime'),
    #         'JourneyPatternRef': get_text(journey, 'txc:JourneyPatternRef'),
    #         #'DaysOfWeek': extract_days_of_week(journey),
    #         #'DaysOfWeek': ', '.join([wd.tag.split('}')[-1] for wd in journey.findall('.//txc:RegularDayType/*', NAMESPACES)]),
    #         'DaysOfWeek': extract_raw_days(journey),
    #         'BankHolidayNonOperation': ', '.join(
    #             [bh.tag.split('}')[-1] for bh in journey.findall('.//txc:BankHolidayOperation/txc:DaysOfNonOperation/*', NAMESPACES)]),
    #         'BankHolidayOperation': ', '.join(
    #             [bh.tag.split('}')[-1] for bh in journey.findall('.//txc:BankHolidayOperation/txc:DaysOfOperation/*', NAMESPACES)])
    #     }
    #     for journey in root.findall('.//txc:VehicleJourney', NAMESPACES)
    # ]

    vehicle_journeys = [
        {
            'DataId': dataid,
            'TicketMachineJourneyCode': get_text(journey, 'txc:Operational/txc:TicketMachine/txc:JourneyCode'),
            'VehicleJourneyCode': get_text(journey, 'txc:VehicleJourneyCode'),
            'PrivateCode': get_text(journey, 'txc:PrivateCode'),
            'OperatorRef': get_text(journey, 'txc:OperatorRef'),
            'ServiceRef': get_text(journey, 'txc:ServiceRef'),
            'LineRef': get_text(journey, 'txc:LineRef'),
            'DepartureTime': get_text(journey, 'txc:DepartureTime'),
            'JourneyPatternRef': get_text(journey, 'txc:JourneyPatternRef'),
            'DaysOfWeek': extract_raw_days(journey),
            'BankHolidayNonOperation': ', '.join(
                [bh.tag.split('}')[-1] for bh in
                 journey.findall('.//txc:BankHolidayOperation/txc:DaysOfNonOperation/*', NAMESPACES)]
            ),
            'BankHolidayOperation': ', '.join(
                [bh.tag.split('}')[-1] for bh in
                 journey.findall('.//txc:BankHolidayOperation/txc:DaysOfOperation/*', NAMESPACES)]
            ),
            'SpecialDaysOperation': [
                (get_text(dr, 'txc:StartDate'), get_text(dr, 'txc:EndDate'))
                for dr in journey.findall('.//txc:SpecialDaysOperation/txc:DaysOfOperation/txc:DateRange', NAMESPACES)
            ],
            'SpecialDaysNonOperation': [
                (get_text(dr, 'txc:StartDate'), get_text(dr, 'txc:EndDate'))
                for dr in journey.findall('.//txc:SpecialDaysOperation/txc:DaysOfNonOperation/txc:DateRange', NAMESPACES)
            ],
            'ServicedOrganisationRef': get_text(
                journey,
                'txc:OperatingProfile/txc:ServicedOrganisationDayType/txc:DaysOfOperation/txc:WorkingDays/txc:ServicedOrganisationRef'
            )
        }
        for journey in root.findall('.//txc:VehicleJourney', NAMESPACES)
    ]

    vehicle_journey_timing_links = [
        {
            'DataId': dataid,
            'VehicleJourneyCode': get_text(journey, 'txc:VehicleJourneyCode'),
            'VehicleJourneyTimingLinkId': link.attrib.get('id') if journey.findall(
                './/txc:VehicleJourneyTimingLink', NAMESPACES) else None,
            'JourneyPatternTimingLinkRef': get_text(link, 'txc:JourneyPatternTimingLinkRef') if journey.findall(
                './/txc:VehicleJourneyTimingLink', NAMESPACES) else None,
            'VehicleJourneyTimingLinkPosition': idx + 1 if journey.findall(
                './/txc:VehicleJourneyTimingLink', NAMESPACES) else None,
            'RunTime': get_text(link, 'txc:RunTime') if journey.findall(
                './/txc:RunTime', NAMESPACES) else None,
            'FromActivity': get_text(link, 'txc:From/txc:Activity') if journey.findall(
                './/txc:From/txc:Activity', NAMESPACES) else None,
            'FromWaitTime': get_text(link, 'txc:From/txc:WaitTime') if journey.findall(
                './/txc:From/txc:WaitTime', NAMESPACES) else None,
            'ToActivity': get_text(link, 'txc:To/txc:Activity') if journey.findall(
                './/txc:To/txc:Activity', NAMESPACES) else None,
            'ToWaitTime': get_text(link, 'txc:To/txc:WaitTime') if journey.findall(
                './/txc:To/txc:WaitTime', NAMESPACES) else None
        }
        for journey in root.findall('.//txc:VehicleJourney', NAMESPACES)
        for idx, link in enumerate(journey.findall('.//txc:VehicleJourneyTimingLink', NAMESPACES) or [{}])
    ]

    return {
        "ServicedOrganisations": pd.DataFrame(serviced_orgs),
        "StopPoints": pd.DataFrame(stop_points),
        "ImportSummary": pd.DataFrame(import_summary),
        "Routes": pd.DataFrame(routes),
        "RouteSections": pd.DataFrame(route_sections),
        "RouteLinks": pd.DataFrame(route_links),
        "JourneyPatterns": pd.DataFrame(journey_patterns),
        "JourneyPatternSections": pd.DataFrame(journey_pattern_sections),
        "JourneyPatternTimingLinks": pd.DataFrame(journey_pattern_timing_links),
        "Operators": pd.DataFrame(operators),
        "Services": pd.DataFrame(services),
        "Lines": pd.DataFrame(lines),
        "VehicleJourneys": pd.DataFrame(vehicle_journeys),
        "VehicleJourneyTimingLinks": pd.DataFrame(vehicle_journey_timing_links),
    }



# Function to read and process all .xml files
def process_all_xml(directory_path):
    dataframes = {
        'ImportSummary': [], 'ServicedOrganisations': [], 'StopPoints': [],'Routes': [],  'RouteSections': [], 'RouteLinks': [], 'JourneyPatterns': [],
        'JourneyPatternSections': [], 'JourneyPatternTimingLinks': [], 'Operators': [], 'Services': [], 'Lines': [], 'VehicleJourneys': [], 'VehicleJourneyTimingLinks': []
    }

    data_set = 0
    for filename in os.listdir(directory_path):
        if filename.endswith('.xml'):
            data_set += 1
            file_path = os.path.join(directory_path, filename)
            parsed_data = process_xml_file(file_path, data_set, filename)

            if parsed_data:  # Ensure parsed_data is not None
                for key in parsed_data:
                    if not parsed_data[key].empty:  # Avoid appending empty DataFrames
                        dataframes[key].append(parsed_data[key])

    # Concatenate all lists into DataFrames
    for key in dataframes:
        if dataframes[key]:  # Ensure the list isn't empty before concatenating
            dataframes[key] = pd.concat(dataframes[key], ignore_index=True)
        else:
            dataframes[key] = pd.DataFrame()  # Return an empty DataFrame if no data was processed

    return dataframes

