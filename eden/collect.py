"""Functions for collecting geographical features for all cities in the US."""

import eden.process as process
import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
from urllib.request import urlretrieve
import shutil
import re
import time
import random
from requests.packages.urllib3.exceptions import InsecureRequestWarning  # MAC
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_places() -> pd.DataFrame:
    """
    Scrapes site for a list of all Places.

    For later scraping we need a complete list of all Place names.
    Place names differ from city names in that they are unique definers.
    They can contain county information when ambigous.

    Parameters
    ----------
    states_name : list[str]
        The names of all states in the current format.
    state_codes : list[str]
        The two letter codes for all the states.

    Returns
    -------
    palce_df : pd.DataFrame
        Pandas dataframe with all Places.

    """
    # Check places data exists, if it does retrieve it and early return
    if os.path.isfile("./data/places.csv"):
        print("Data for Places exists.")
        df = pd.read_csv("./data/places.csv")
        return df
    print(f"No Places data exists.")

    # Get the states names and two letter codes from reference
    state_dict = process.state_codes()
    state_codes = list(state_dict.keys())
    state_names = list(state_dict.values())

    # The base url for searching for a states
    base_state_url = "https://www.bestplaces.net/find/state.aspx?state="

    # List of lists for creating the final dataframe and csv file
    place_lol: list[list[str, str, str]] = []

    # Loop through all state pages using the base url and each state code
    for index, state_code in enumerate(state_codes):
        print(f"Retrieving Places for {state_names[index]}.")
        result = requests.get(base_state_url + state_code, verify=False)
        doc = BeautifulSoup(result.text, "html.parser")

        # Select the div containing the place list and grab name from end of href
        places_div = doc.find_all("div", class_="col-md-4")
        for place_div in places_div:
            places = place_div.find_all("a", href=True)
            for place in places:
                place_url = place["href"]
                place = place_url.split("/")[-1]
                place_lol.append([place, state_code])

    # This converts the dictionary to a csv with place and state columns
    place_df = pd.DataFrame(place_lol, columns=["Place", "StateCode"])
    place_df.to_csv("./data/places.csv", index=False)

    return place_df


def get_counties(place_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds scraped county data.

    The Places identifier is unambigous.
    However, to use City names we will nead the county

    Parameters
    ----------
    place_df : pd.DataFrame
        Dataframe with Place identifiers from BestPlaces.

    Returns
    -------
    county_df : pd.DataFrame
        place_df with additional county information added.
    """

    # Look for county checkpoint data, finished file, or no data
    if os.path.isfile("./data/temp/counties_raw.csv"):
        print("Counties data exists.")
        county_df = pd.read_csv("./data/temp/counties_raw.csv", keep_default_na=False)
        return county_df
    elif os.path.isfile("./data/temp/counties_checkpoint.csv"):
        print("Partial counties data exists.")
        county_df = pd.read_csv("./data/temp/counties_checkpoint.csv", keep_default_na=False)
    else:
        print("No counties data exists.")
        county_df = place_df.assign(County="").reset_index(drop=True)

    # Loop through the counties dataframe to generate url skip if already exists
    base_place_url = "https://www.bestplaces.net/city/"
    state_dict = process.state_codes()
    for index, row in county_df.iterrows():
        place = row["Place"]
        code = row["StateCode"]
        state = state_dict[code]
        county = row["County"]

        # If the county has already been found, continue
        if county:
            continue

        # Identify and format the county name
        result = requests.get(f"{base_place_url}/{state}/{place}", verify=False)
        doc = BeautifulSoup(result.text, "html.parser")
        county_parent = doc.find("b", text=re.compile(r'County:'))
        # Skip cities that return a 401 error and label with a "?"
        if county_parent != None:
            county_raw = county_parent.find_next_sibling().find("a").text
            county = county_raw.strip().lower()
        else:
            county = "?"
        county_df.loc[index, "County"] = county

        # Save the counties out to a checkpoint file
        print(f"Collected {place}, {code}")
        county_df.to_csv("./data/temp/counties_checkpoint.csv", index=False)

    # Save out the finalized data and delete the checkpoint file
    county_df.to_csv("./data/temp/counties_raw.csv", index=False)
    os.remove("./data/temp/counties_checkpoint.csv")

    return county_df


def download_geodata() -> pd.DataFrame:
    """
    Retrieves geographical data such as zip codes, county, and latitude.

    This function only retrieves the data and does not add it to the final df.

    Returns
    -------
    raw_geodata_df : pd.DataFrame
        The raw downloaded geodata.
    """

    # Check geodata data exists, if it does retrieve it and early return
    unpack_loc = "data/temp"
    if os.path.isfile(f"{unpack_loc}/geodata_raw.csv"):
        print(f"Geodata exists.")
        df = pd.read_csv(f"{unpack_loc}/geodata_raw.csv")
        return df
    print("No geodata exists.")

   # File locations for the downloaded zip code data and its contents
    print("Downloading geographical.")
    url = "https://simplemaps.com//static/data/us-cities/1.75/basic/simplemaps_uscities_basicv1.75.zip"
    zip_loc = f"{unpack_loc}/geodata.zip"
    urlretrieve(url, zip_loc)

    # Unpack the zip file and then delete the unused files
    shutil.unpack_archive(zip_loc, unpack_loc)
    os.remove(f"{unpack_loc}/license.txt")
    os.remove(f"{unpack_loc}/geodata.zip")
    os.remove(f"{unpack_loc}/uscities.xlsx")
    os.rename(f"{unpack_loc}/uscities.csv", f"{unpack_loc}/geodata_raw.csv")

    # Read in the CSV file and delete unused columns and incomplete rows
    raw_geodata_df = pd.read_csv(f"{unpack_loc}/geodata_raw.csv")

    # return raw_geodata_df
    return raw_geodata_df


def get_geodata(county_df: pd.DataFrame, raw_geodata_df: pd.DataFrame) -> pd.DataFrame:
    """
    Combines the zipcode data into the growing cities-oriented pandas dataframe.

    Parameters
    ----------
    county_df : pd.DataFrame
        The growing dataframe with the cities, states, statecodes, and counties.
    raw_geodata_df : pd.DataFrame
        The raw downloaded geodata.

    Returns
    -------
    geodata_df : pd.DataFrame
        Merged data from with county and geodata.
    """

    # Clean up raw geodata
    columns_to_drop = ["city_ascii", "source", "military", "incorporated", "timezone", "ranking", "id"]
    geodata_df = raw_geodata_df.drop(columns_to_drop, axis=1)
    geodata_df = geodata_df.dropna()

    # Update column names
    geodata_df.columns = ['City', 'StateCode', 'State', 'Fips', 'County',
                          'Latitude', 'Longitude', 'Population', 'Density', 'Zip']

    # Correct formating for the City, State and County names
    columns_to_format = ["City", "State", "County", "StateCode"]
    for column in columns_to_format:
        geodata_df[column] = geodata_df[column].str.lower()
        geodata_df[column] = geodata_df[column].str.replace(" ", "_")

    print("Saving geographical data to geodata.csv")
    geodata_df.to_csv("./data/geodata.csv", index=False)

    geodata_df = pd.merge(county_df, geodata_df, on=["City", "State", "StateCode", "County"])
    geodata_df.to_csv("merged.csv")

    dropped = county_df.merge(geodata_df, indicator=True, on=[
        "City", "State", "StateCode"], how='left').loc[lambda x: x['_merge'] != 'both']
    dropped.to_csv("dropped.csv")

    return geodata_df
