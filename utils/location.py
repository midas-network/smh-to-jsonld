# State abbreviation mapping
STATE_ABBR = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'District of Columbia': 'DC',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL',
    'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
    'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR',
    'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA',
    'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'American Samoa': 'AS', 'Guam': 'GU', 'Northern Mariana Islands': 'MP',
    'Puerto Rico': 'PR', 'Virgin Islands': 'VI', 'United States': 'US'
}

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
def get_location_info(fips_code):
    """Generate location information for a given FIPS code."""
    fips_code = str(fips_code)
    location_name = get_location_from_fips(fips_code)

    # Extract state name for state-level FIPS
    if len(fips_code) == 2 or (len(fips_code) == 5 and fips_code[2:] == '000'):
        state_code = fips_code[:2]
        state_name = STATE_FIPS.get(state_code, "Unknown")
    else:
        # For county FIPS, extract state
        state_code = fips_code[:2]
        state_name = STATE_FIPS.get(state_code, "Unknown")

    # Create a geonames-like ID
    geonames_id = f"fips_{fips_code}"

    location_info = {
        "@context": {
            "iso3166-1": "http://www.iso.org/iso-3166-1#",
            "iso3166-2": "http://www.iso.org/iso-3166-2#",
            "gn": "http://www.geonames.org/ontology#",
            "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#"
        },
        "@id": f"http://sws.geonames.org/{geonames_id}/",
        "@type": "gn:Feature",
        "gn:name": location_name,
        "iso3166-1:alpha2": "US",
        "iso3166-1:alpha3": "USA",
        "iso3166-1:numeric": "840",
        "gn:fipsCode": fips_code
    }

    # Add state abbreviation if available
    state_abbr = STATE_ABBR.get(state_name, "")
    if state_abbr:
        location_info["iso3166-2:code"] = f"US-{state_abbr}"

    return location_info