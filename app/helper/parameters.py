region = 'West Yorkshire'
rte_version = 'WYOR-IMP'
runtime_version = 'WYOR-IMP'
vsc_name = 'IMP'
description  = 'Import from TXC'

service_type = 0
service_mode = 0
smoothing = 4

imperial = True

SOURCE_CRS = 'EPSG:4326'   # Input KML/GeoData default
TARGET_CRS = 'EPSG:27700'  # British National Grid (or set your desired CRS)

NAMESPACES = {'txc': 'http://www.transxchange.org.uk/'}

StartDate = '2025-04-01'
EndDate = '2025-04-31'


# Sample Regular Week - 7 days
reg_week = ('2025-04-01', '2025-04-07')  # Monday to Sunday


# Sample Holiday Week
hol_week = ('2025-04-01', '2025-04-04')  # Monday to Friday



sched_type_code = {
    "Weekday": 0,
    "Saturday": 5,
    "Sunday": 6,
    "Holiday": 7,
    "AllWeek": 16,
    "Weekend": 17
}


day_type_code = {
    "Weekday": 1,
    "Saturday": 6,
    "Sunday": 7,
    "Holiday": "H",
    "AllWeek": "A",
    "Weekend": "W",
    "School": "S"
}

service_type_nickname = {
    "Weekday": "WKD",
    "Saturday": "SAT",
    "Sunday": "SUN",
    "Holiday": "HOL",
    "AllWeek": "ALL",
    "Weekend": "WKE",
    "School": "SCH"
}

days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

column_mapping = { 'RouteId': 'VariantId',
                   'RouteRef': 'VariantId',
                   'FileRoute': 'RouteId',
                   'StopPointRef': 'StopPointId',
                   'RouteLinkRef': 'RouteLinkId',
                   'JourneyPatternSectionRefs': 'JourneyPatternSectionId',
                   'LineRef': 'LineId',
                   'JourneyPattern': 'JourneyPatternId',
                   'JourneyPatternRef': 'JourneyPatternId',
                   'FromStopPointRef': 'FromStopPointId',
                   'ToStopPointRef': 'ToStopPointId',
                   'RegisteredOperatorRef': 'OperatorId',
                   'OperatorRef': 'OperatorId',
                   'ServiceRef': 'ServiceCode',
                   'JourneyPatternTimingLinkRef': 'JourneyPatternTimingLinkId',
                   'ServicedOrganisationRef': 'OrganisationCode'
                   }
