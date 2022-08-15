"Functions for processing and formating collected data."


def state_codes() -> dict:
    """
    Retrieve a dictionary of state codes and names.

    Returns
    -------
    state_dict : dict[str:str]
        Dictionary with state clodes as keys and state names as values.
    """

    # Conversion dictionary for state codes and names
    state_dict = {"al": "alabama",
                  "ak": "alaska",
                  "az": "arizona",
                  "ar": "arkansas",
                  "ca": "california",
                  "co": "colorado",
                  "ct": "connecticut",
                  "de": "delaware",
                  "fl": "florida",
                  "ga": "georgia",
                  "hi": "hawaii",
                  "id": "idaho",
                  "il": "illinois",
                  "in": "indiana",
                  "ia": "iowa",
                  "ks": "kansas",
                  "ky": "kentucky",
                  "la": "louisiana",
                  "me": "maine",
                  "md": "maryland",
                  "ma": "massachusetts",
                  "mi": "michigan",
                  "mn": "minnesota",
                  "ms": "mississippi",
                  "mo": "missouri",
                  "mt": "montana",
                  "ne": "nebraska",
                  "nv": "nevada",
                  "nh": "new_hampshire",
                  "nj": "new_jersey",
                  "nm": "new_mexico",
                  "ny": "new_york",
                  "nc": "north_carolina",
                  "nd": "north_dakota",
                  "oh": "ohio",
                  "ok": "oklahoma",
                  "or": "oregon",
                  "pa": "pennsylvania",
                  "ri": "rhode_island",
                  "sc": "south_carolina",
                  "sd": "south_dakota",
                  "tn": "tennessee",
                  "tx": "texas",
                  "ut": "utah",
                  "vt": "vermont",
                  "va": "virginia",
                  "wa": "washington",
                  "wv": "west_virginia",
                  "wi": "wisconsin",
                  "wy": "wyoming",
                  "dc": "district_of_columbia"
                  }

    return state_dict
