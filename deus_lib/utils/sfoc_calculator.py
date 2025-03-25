import pandas as pd
import numpy as np
from scipy.interpolate import interp1d

limits = {
    '9102095': {  # Stolt Creativity
        'mrc_value': 80
    },
    '9178202': {  # Stolt Effort
        'mrc_value': 80
    },
    '9124469': {  # Stolt Achievement
        'mrc_value': 80
    },
    '9178197': {  # Stolt Concept
        'mrc_value': 80
    },
    '9414060': {  # Stolt Norland
        'mrc_value': 80
    },
    '9414084': {  # Stolt Breland
        'mrc_value': 80
    },
    '9680102': {  # Stolt Tenacity
        'mrc_value': 80
    }
}

accepted_imos = list(limits)

sfoc_values = {}

def get_sfoc_ideals(shipID, s3_controller):
    imo =str(shipID)
    # get valid file path of the trim table
    # Check if the vessel exists in sfoc_values
    if imo not in sfoc_values:
        full_table = s3_controller.download_sfoc_csv_file('deus-trim-tables', 'emo_sfoc.csv')
        full_table['imo']= full_table['imo'].astype(str)
        # Get the SFOC curves for the given ship
        sfoc_table_imo = full_table[full_table['imo'] == shipID]

        sfoc_curves = interp1d(sfoc_table_imo['mrc_load'], sfoc_table_imo['sfoc'], bounds_error=False)
        sfoc_min_curves = interp1d(sfoc_table_imo['mrc_load'], sfoc_table_imo['sfoc_tolereance_min'], bounds_error=False)
        sfoc_max_curves = interp1d(sfoc_table_imo['mrc_load'], sfoc_table_imo['sfoc_tolereance_max'], bounds_error=False)
        
        # Calculate the SFOC and SFOC Tolerances for the given MRC
        sfoc_st = np.round(sfoc_curves(limits[imo]['mrc_value']),6)
        sfoc_min_st = np.round(sfoc_min_curves(limits[imo]['mrc_value']),6)
        sfoc_max_st = np.round(sfoc_max_curves(limits[imo]['mrc_value']),6)
        
        # Store the values in the dictionary
        sfoc_values[imo] = {
            'sfoc_st': sfoc_st,
            'sfoc_min_st': sfoc_min_st,
            'sfoc_max_st': sfoc_max_st
        }
    return sfoc_values[imo]
