
def get_location_from_fips(fips_code):
    """
    Convert a FIPS (Federal Information Processing Standards) code to a location name.

    Args:
        fips_code (str or int): FIPS code (2-digit for state, 5-digit for county)

    Returns:
        str: Human-readable location name
    """
    # Just convert to string without padding
    fips_code = str(fips_code)

    # Handle state-level FIPS
    if len(fips_code) == 2 or (len(fips_code) == 5 and fips_code[2:] == '000'):
        state_code = fips_code[:2]
        if state_code in STATE_FIPS:
            return STATE_FIPS[state_code]
        else:
            return f"Unknown state code: {state_code}"

    # Handle county-level FIPS (5 digits)
    elif len(fips_code) == 5:
        state_code = fips_code[:2]
        county_code = fips_code[2:]

        state_name = STATE_FIPS.get(state_code, f"Unknown state ({state_code})")

        # Check for known county
        return f"County code {county_code}, {state_name}"

    # Invalid FIPS format
    else:
        return f"Invalid FIPS code: {fips_code}"

# Dictionary of state FIPS codes
STATE_FIPS = {
    '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas',
    '06': 'California', '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware',
    '11': 'District of Columbia', '12': 'Florida', '13': 'Georgia', '15': 'Hawaii',
    '16': 'Idaho', '17': 'Illinois', '18': 'Indiana', '19': 'Iowa',
    '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine',
    '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
    '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska',
    '32': 'Nevada', '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico',
    '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio',
    '40': 'Oklahoma', '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island',
    '45': 'South Carolina', '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas',
    '49': 'Utah', '50': 'Vermont', '51': 'Virginia', '53': 'Washington',
    '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming',
    '60': 'American Samoa', '66': 'Guam', '69': 'Northern Mariana Islands',
    '72': 'Puerto Rico', '78': 'Virgin Islands', "US": "United States"
}

# Dictionary of county FIPS codes (limited to major counties for brevity)
# In a real-world application, this would be more comprehensive or loaded from