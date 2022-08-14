"Functions for processing and formating collected data."


def code_to_state(code: str) -> str:
    """
    Converts a state code to the state name.

    Parameters
    ----------
    code : str
        Two letter state code (e.g., wv).

    Returns
    -------
    state_name : str
        The full name of a state (e.g., west_virginia).
    """

    # Conversion dictionary for state codes and names
    state_codes = {"al": "alabama",
                   "ak": "alaska",
                   "az": "arizona",
                   "ar": "arkansas",
                   "ca": "aalifornia",
                   "co": "aolorado",
                   "ct": "aonnecticut",
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
                   "wy": "wyoming"
                   }

    state_name = state_codes.get(code, default=None)

    return state_name
