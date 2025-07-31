import os
import pandas as pd
import numpy as np
from geopy.distance import geodesic
from itertools import islice
import re
import math

def import_stops(file_path, delimiter):

    stops = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)
    stops.rename(columns={'ATCOCode': 'StopPointId'}, inplace=True)

    return stops

def remove_first_vowel(word):
    """Remove the first vowel that is not the first letter of the word."""
    if len(word) < 2:
        return word  # No change for single-character words

    first_letter, rest = word[0], word[1:]
    updated_rest = re.sub(r'[AEIOUaeiou]', '', rest, count=1)

    return first_letter + updated_rest

# Read file to dataframe C:\Users\blckm\PycharmProjects\msa-hastus-gtfs-processor\Inputs\stops_needing_places.txt
data_stops_path = r"C:\Users\blckm\PycharmProjects\msa-transxchange-to-hastus\InputFiles\stops"
#stops_file = os.path.join(data_stops_path, "minimal_route_points.csv")
data_stops_file = os.path.join(data_stops_path, "stops_by_route2.csv")
#stops_path = r"C:\Users\blckm\PycharmProjects\msa-transxchange-to-hastus\InputFiles"
stops_file = os.path.join(data_stops_path, "stop-codes.csv")

stops = import_stops(stops_file, ",")

#stops = import_stops('InputFiles/stops/stop-codes.csv', ",")


data_stops = import_stops(data_stops_file, ",")

data_stops = data_stops[['StopPointId']]
# Merge data_stops with stops
data_stops = pd.merge(data_stops, stops, on='StopPointId', how='left')

Mapping = {
            "BCT": "BusCoachTrolleyStopOnStreet",
            "BCE": "BusCoachTrolleyStationEntance",
            "BCS": "BusCoachTrolleyStationBay",
            "BST": "BusCoachAccess",
            "BCQ": "BusCoachTrolleyStationVariableBay",
            "FER": "FerryOrPortAccess",
            "MET": "TramMetroUndergroundAccess",
            "PLT": "TramMetroUndergroundPlatform",
            "TMU": "TramMetroUndergroundEntrance",
            "TXR": "TaxiRank",
            "STR": "Street",
            "SDA": "CarSetDownPickUpArea",
            "AIR": "AirportEntrance",
            "GAT": "AirAccessArea",
            "FTD": "FerryTerminalDockEntrance",
            "FBT": "FerryOrPortBerth",
            "RSE": "RailStationEntrance",
            "RLY": "RailAccess",
            "RPL": "RailPlatform",
            "LCE": "LiftOrCableCarStationEntrance",
            "LCB": "LiftOrCableCarAccessArea",
            "LPL": "CarSetDownPickUpArea"
            }

# Abbreviation mapping
ABBREVIATIONS = {
    "Street": "St", "Lane": "Ln", "Road": "Rd", "Avenue": "Av",
    "Drive": "Dr", "Court": "Ct", "Place": "Pl", "Boulevard": "Bl",
    "Highway": "Hwy", "Parade": "Pde", "Square": "Sq", "Crescent": "Cr",
    "Close": "Cl", "Terrace": "Ter", "Gardens": "Gdn", "Park": "Pk", "North": "N", "South": "S", "East": "E", "West": "W",
    "Shopping Centre": "SC", "Bus Station": "BS", "Railway Station": "RS", "Bus Stn": "BS", "The": "", "And": "",
    "Sainsburys": "Sain", "Tesco": "Tes", "Asda": "Asd", "Morrisons": "Mor", "Aldi": "Ald", "ALDI": "Ald", "Lidl": "Lid",
    "Metrolink Stop": "ML", "METROLINK STOP": "ML", "IMW Metrolink Stop": "ML", "Tram Stop": "TS",  "Stop": "", "STOP": "",
    "Primary School": "PS", "Secondary School": "SS", "College": "Col", "High School": "HS",
    "Primary Sch": "PS", "High Sch": "HS", "School": "Sch", "Church": "", "Hospital": "Hsp", "Library": "Lib", "PH": "",
}


STREET_ABBREVIATIONS = {
    "Street": "St", "Lane": "Ln", "Road": "Rd", "Avenue": "Av",
    "Drive": "Dr", "Court": "Ct", "Place": "Pl", "Boulevard": "Bl",
    "Highway": "Hwy", "Parade": "Pde", "Square": "Sq", "Crescent": "Cr",
    "Close": "Cl", "Terrace": "Ter"
}


# Generic names that need extra context
GENERIC_NAMES = {"Station", "Metrolink Stop", "IMW Metrolink Stop", "METROLINK STOP", "Tram Stop", "School", "Shopping Centre", "Bus Station", "Bus Stn", "Railway Station", "Sainsburys", "Tesco", "Asda", "Morrisons" "Aldi", "Lidl", ""}


def abbreviate_phrase(name):
    if not isinstance(name, str) or not name.strip():
        return ""

    words = name.split()
    phrase = " ".join(words)

    # Track original and abbreviated words
    replacements = []
    for phrase_key, abbr in sorted(ABBREVIATIONS.items(), key=lambda x: -len(x[0])):
        # Find all matching phrases using word boundaries
        matches = list(re.finditer(rf"\b{re.escape(phrase_key)}\b", phrase, flags=re.IGNORECASE))
        for match in matches:
            start, end = match.span()
            replacements.append((start, end, abbr, phrase_key))

    # Apply replacements from end to start so positions don't shift
    for start, end, abbr, _ in sorted(replacements, key=lambda x: -x[0]):
        phrase = phrase[:start] + abbr + phrase[end:]

    abbreviated = phrase.strip()

    # If abbreviated phrase is too short, revert substitutions starting from the beginning
    if len(abbreviated) < 6:
        phrase = name  # Reset to original
        for i in range(len(replacements)):
            temp_phrase = phrase
            for j, (start, end, abbr, phrase_key) in enumerate(replacements[i:], start=i):
                temp_phrase = re.sub(rf"\b{re.escape(phrase_key)}\b", abbr, temp_phrase, flags=re.IGNORECASE)
            if len(temp_phrase.strip()) >= 6:
                return temp_phrase.strip()
        return name.strip()  # If still too short, return full original
    else:
        return abbreviated




def abbreviate_name(name):
    """Generate a 6-character stop code while preserving word order."""
    if not isinstance(name, str) or not name.strip():
        return ""

    # words = name.split()
    # remaining_chars = []  # Stores characters from the first non-abbreviated word
    # abbreviation_suffix = ""
    #
    # # Step 1: Detect and extract multi-word abbreviations
    # phrase = " ".join(words)  # Join words for phrase-matching
    # for phrase_key, abbr in ABBREVIATIONS.items():
    #     if phrase_key in phrase:
    #         abbreviation_suffix = abbr  # Store abbreviation (e.g., "BS" for "Bus Station")
    #         phrase = phrase.replace(phrase_key, " " + abbr + " ").strip()  # Remove it from main phrase

    phrase = abbreviate_phrase(name)

    # Step 2: Extract first non-abbreviated word
    words = phrase.split()
    text_length = 0
    extra_word = 0
    extra_count = 0
    main_word = ""
    second_word = ""
    rem_char = 0
    main_len = 0
    sec_lne = 0
    result = ""

    for word in words:
        if word in ABBREVIATIONS.values():
            text_length = text_length + len(word)
        else:
            if main_word is "":
                extra_word += 1
                main_word = word  # First valid non-abbreviated word
            else:
                if second_word is "":
                    extra_word += 1
                    second_word = word  # First valid non-abbreviated word

    rem_char = 6 - text_length



    main_len = math.ceil(rem_char/extra_word)
    main_len = max(main_len, rem_char - len(second_word))
    main_len = min(main_len, len(main_word))
    sec_len = math.floor(rem_char/extra_word)
    sec_len = max(sec_len, rem_char - len(main_word))
    sec_len = min(sec_len, len(second_word))

    for word in words:
        if word in ABBREVIATIONS.values():
            result += word
        elif word == main_word:
            if rem_char > 0:
                result += word[0:main_len]
        elif word == second_word:
            if rem_char > 0:
                result += word[0:sec_len]

    result = result[:6]

    return result


def stops_are_close(coord1, coord2, threshold_km=0.1):
    """Check if two stops are close using geodesic distance."""
    return geodesic(coord1, coord2).km <= threshold_km

def generate_stop_codes(df):
    """Generate stop codes and handle duplicates based on proximity."""
    df["StopCode"] = ""
    df["DuplicateCode"] = False
    df["UsedCols"] = ""
    seen_codes = {}

    for i, row in df.iterrows():
        name = row["CleanShortCommonName"] if isinstance(row["CleanShortCommonName"], str) else ""
        used_cols = []

        if name in GENERIC_NAMES or not name.strip() or len(name) < 6:
            extra_context = ""
            for extra_col in ["CommonName", "LocalityName", "Street", "Landmark", "Descriptions"]:
                if pd.notna(row[extra_col]) and row[extra_col] not in GENERIC_NAMES:
                    name = str(row[extra_col])
                    used_cols.append(extra_col)
                    break
                elif row[extra_col] in GENERIC_NAMES and not extra_context:
                    extra_context = row[extra_col]
                    used_cols.append(extra_col)

            if extra_context and extra_context not in name:
                name = f"{name} {extra_context}"

        stop_code = abbreviate_name(name)
        normalized_code = stop_code.lower()  # 🧠 normalize here

        while normalized_code in seen_codes:
            other_idx, other_coords = seen_codes[normalized_code]

            if stops_are_close((row["Latitude"], row["Longitude"]), other_coords):
                break

            for col in ["LocalityName", "Town", "Street"]:
                if col not in used_cols and pd.notna(row[col]):
                    name = f"{name} {row[col]}"
                    stop_code = abbreviate_name(name)
                    normalized_code = stop_code.lower()  # 🔁 re-normalize
                    used_cols.append(col)
                    seen_codes[normalized_code] = (i, (row["Latitude"], row["Longitude"]))

                    if stops_are_close((row["Latitude"], row["Longitude"]), seen_codes[normalized_code][1]):
                        break

            if normalized_code in seen_codes:
                name = remove_first_vowel(name)
                stop_code = abbreviate_name(name)
                normalized_code = stop_code.lower()  # 🔁 re-normalize
                seen_codes[normalized_code] = (i, (row["Latitude"], row["Longitude"]))

                if stops_are_close((row["Latitude"], row["Longitude"]), seen_codes[normalized_code][1]):
                    break

            if normalized_code in seen_codes:
                df.at[i, "DuplicateCode"] = True
                break

        df.at[i, "StopCode"] = stop_code
        df.at[i, "UsedCols"] = ", ".join(used_cols)
        seen_codes[normalized_code] = (i, (row["Latitude"], row["Longitude"]))

    return df


def generate_stop_codes_not_sure(df):
    """Generate stop codes and handle duplicates based on proximity."""
    df["StopCode"] = ""
    df["DuplicateCode"] = False
    df["UsedCols"] = ""
    seen_codes = {}

    for i, row in df.iterrows():
        name = row["CleanShortCommonName"] if isinstance(row["CleanShortCommonName"], str) else ""
        # If name is generic or missing, find a better one
        if name in GENERIC_NAMES or not name.strip():
            extra_context = ""
            for extra_col in ["CommonName", "Landmark", "Street", "LocalityName"]:
                if pd.notna(row[extra_col]) and row[extra_col] not in GENERIC_NAMES:
                    used_cols = used_cols.append(extra_col)
                    name = str(row[extra_col])
                    break
                elif row[extra_col] in GENERIC_NAMES and not extra_context:
                    extra_context = row[extra_col]  # Store first found generic name

            # Append only one instance of the generic term
            if extra_context and extra_context not in name:
                name = f"{name} {extra_context}"

        stop_code = abbreviate_name(name)

        # Handle duplicates
        attempt = 1
        while stop_code in seen_codes:
            other_idx, other_coords = seen_codes[stop_code]
            if stops_are_close((row["Latitude"], row["Longitude"]), other_coords):
                break  # If stops are close, it's not a real duplicate

            # Try alternative generation method
            if attempt == 1:
                stop_code = abbreviate_name(name[1:])  # Skip first vowel
            elif attempt == 2:
                stop_code = abbreviate_name(name[::-1])  # Reverse order
            else:
                df.at[i, "DuplicateCode"] = True  # Mark as duplicate
                break

            attempt += 1  # Increase attempt counter

        seen_codes[stop_code] = (i, (row["Latitude"], row["Longitude"]))
        df.at[i, "StopCode"] = stop_code
    return df


def clean_short_common_name(name):
    if not isinstance(name, str) or not name.strip():
        return ""

    words = name.split()

    # Collect both keys and values from dictionaries
    abbreviations_set = set(ABBREVIATIONS.keys()).union(ABBREVIATIONS.values())
    generic_names_set = set(GENERIC_NAMES).union(ABBREVIATIONS.values())
    street_abbreviations_set = set(STREET_ABBREVIATIONS.keys()).union(STREET_ABBREVIATIONS.values())

    # Identify if at least one word is NOT in ABBREVIATIONS or GENERIC_NAMES
    contains_non_generic = any(
        word not in abbreviations_set and word not in generic_names_set for word in words if word.lower() != "the"
    )

    # Remove "the" only if at least one remaining word is NOT in ABBREVIATIONS or GENERIC_NAMES
    if contains_non_generic:
        words = [word for word in words if word.lower() != "the"]

    # Check again if at least one remaining word is NOT in ABBREVIATIONS or GENERIC_NAMES
    contains_non_generic_after_the = any(
        word not in abbreviations_set and word not in generic_names_set for word in words
    )

    # Compute total length of remaining words
    total_length_before = sum(len(word) for word in words)

    # Remove words in STREET_ABBREVIATIONS if:
    # - At least one remaining word is NOT in ABBREVIATIONS or GENERIC_NAMES
    # - There is more than one remaining word after removal
    if contains_non_generic_after_the:
        filtered_words = [word for word in words if word not in street_abbreviations_set]

        # Ensure at least one non-generic/non-abbreviation word remains
        remaining_non_generic = [
            word for word in filtered_words if word not in abbreviations_set and word not in generic_names_set
        ]

        # If only **one word remains**, **restore** street abbreviations
        if len(remaining_non_generic) <= 1:
            words = words  # Keep original words (don't remove street names)
        else:
            words = filtered_words  # Apply filtering if multiple words remain

    return " ".join(words)


df = data_stops.copy(deep=True)



df[["ShortCommonName", "CommonName", "Landmark", "LocalityName", "Street"]] = (
    df[["ShortCommonName", "CommonName", "Landmark", "LocalityName", "Street"]]
    .applymap(lambda x: re.sub(r'[^\w\s]', ' ', str(x)) if pd.notna(x) else x)
)

#df["CleanShortCommonName"] = df["ShortCommonName"].apply(clean_short_common_name)

df["CleanShortCommonName"] = df["ShortCommonName"]

#df = df.loc[[7415]].reset_index(drop=True)



# Apply function to assign unique stop codes and detect duplicates
df = generate_stop_codes(df)
