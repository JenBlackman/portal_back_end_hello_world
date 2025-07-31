import os
import pandas as pd
from process_txc import transform_txc
from process_txc.read_txc import process_xml_file  # Ensure correct import

def process_all_xml(directory_path):

    # Initialize dictionaries to store both raw and transformed data
    original_dataframes = {
        'ServicedOrganisations': [], 'StopPoints': [], 'Routes': [], 'RouteSections': [], 'RouteLinks': [],
        'JourneyPatterns': [], 'JourneyPatternSections': [], 'JourneyPatternTimingLinks': [], 'Operators': [],
        'Services': [], 'Lines': [], 'VehicleJourneys': [], 'VehicleJourneyTimingLinks': []
    }

    transformed_dataframes = {
        'VariantSections': [], 'VariantMapping': [], 'VariantLinks': [], 'VariantPoints': [], 'Trips': [],
        'TripSections': [], 'TripStops': []
    }

    # Process each XML file
    for filename in os.listdir(directory_path):
        if filename.endswith('.xml'):
            file_path = os.path.join(directory_path, filename)
            print(f"Processing {filename}...")

            # Step 1: Read the XML file and extract original data
            extracted_data = process_xml_file(file_path)
            # Step 2: Transform the extracted data
            transformed_data = transform_txc.transform_all_txc_tables(extracted_data)

            # Step 3: Append extracted and transformed data to their respective lists
            for key, df in extracted_data.items():
                original_dataframes[key].append(df)

            for key, df in transformed_data.items():
                if key not in transformed_dataframes:
                    transformed_dataframes[key] = []  # Initialize if missing
                transformed_dataframes[key].append(df)

            print(f"Completed processing {filename}.\n")

    # Step 4: Concatenate lists into final DataFrames
    for key in original_dataframes:
        if original_dataframes[key]:  # Only concatenate if there is data
            original_dataframes[key] = pd.concat(original_dataframes[key], ignore_index=True)
        else:
            original_dataframes[key] = pd.DataFrame()  # Set empty DataFrame if no data

    for key in transformed_dataframes:
        if transformed_dataframes[key]:  # Only concatenate if there is data
            transformed_dataframes[key] = pd.concat(transformed_dataframes[key], ignore_index=True)
        else:
            transformed_dataframes[key] = pd.DataFrame()  # Set empty DataFrame if no data

    return original_dataframes, transformed_dataframes