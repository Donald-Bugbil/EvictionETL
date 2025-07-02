import pandas as pd



#A function to convert all the boolen to 1 and 0 representingthe True/false respectively from the raw data
def boolens(value):
    if value is True:
        return 1
    else:
        return 0
    
#This function cleans the zipcode
def clean_zip(val):
    try:
        if pd.isnull(val):
            return ''
        
        val = int(float(val))  # Handles strings and floats like '94110.0'

        # San Francisco ZIP code range
        if 94102 <= val <= 94188:
            return f"{val:05d}"
        else:
            return ''  # Not a valid SF ZIP
    except:
        return ''  # Invalid or malformed input
    
#cleaning the State
def state(name):
    if pd.isna(name):
        return 'CA'
    name = name.strip()
    if name in ['Ca', 'CA`', 'nan']:
        return 'CA'
    return name

#cleaning City
def clean_city(name):
    if pd.isna(name):
        return 'San Francisco'
    name=name.strip()
    if name in ['San Franicsco','nan', 'San Frnaicsco','San Franisco','San Francicso','San Franciso','San Frnacisco' ,'San  Frnacisco',
                'San ‘francisco','C' ,'3/9/2017', 'San Frncisco', 'La Canada','San Francisoc' ,'Tom Francisco', 'Sn Francisco', 
                '399 Haight Street', '459 Turk Street', 'San Francisc', 'San Francisco`', '158an Francisco']:
        return 'San Francisco'
    return name

# Extract Latitude and Longitude from the 'Location' column    
def extract_lat_lon(location_str):
    try:
        # Remove parentheses and split
        lat, lon = location_str.strip("()").split(",")
        return pd.Series([float(lat), float(lon)])
    except:
        return pd.Series([None, None])  # Handle malformed entries
    
# Extract Latitude and Longitude from the 'Shape' column    
def extract_lat_lon_from_shape(shape_str):
    try:
        # Remove 'POINT (' and ')' and split
        lon, lat = shape_str.replace("POINT", "").strip(" ()").split()
        return pd.Series([float(lat), float(lon)])
    except:
        return pd.Series([None, None])
