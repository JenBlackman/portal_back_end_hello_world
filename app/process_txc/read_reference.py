import pandas as pd

def import_stops(file_path):

    # import stop points
    stops = pd.read_csv(file_path, low_memory=False)
    stops = stops[['ATCOCode', 'NaptanCode', 'CommonName', 'Latitude', 'Longitude']]
    stops.rename(columns={'ATCOCode': 'StopPointRef'}, inplace=True)

    return stops