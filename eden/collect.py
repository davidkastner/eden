"""Functions for collecting geographical features for all cities in the US."""

import eden.collect as process
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
    state_names = list(pd.read_csv("./data/states.csv")["State"])
    state_codes = list(pd.read_csv("./data/states.csv")["StateCode"])

    # The base url for searching for a states
    base_state_url = "https://www.bestplaces.net/find/state.aspx?state="

    # List of lists for creating the final dataframe and csv file
    place_lol: list[list[str, str, str]] = []

    # Loop through all state pages using the base url and each state code
    for index, state_code in enumerate(state_codes):
        print(f"Retrieving cities for {state_names[index]}.")
        result = requests.get(base_state_url + state_code, verify=False)
        doc = BeautifulSoup(result.text, "html.parser")

        # Select the div containing the place list and grab name from end of href
        places_div = doc.find_all("div", class_="col-md-4")
        for place_div in places_div:
            places = place_div.find_all("a", href=True)
            for place in places:
                place_url = place["href"]
                place = place_url.split("/")[-1]
                place_lol.append([place, state_names[index], state_code])

    # This converts the dictionary to a csv with place and state columns
    place_df = pd.DataFrame(place_lol, columns=["Place", "State", "StateCode"])
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
    if os.path.isfile("./data/counties.csv"):
        print("Counties data exists.")
        county_df = pd.read_csv("./data/counties.csv", keep_default_na=False)
        return county_df
    elif os.path.isfile("./data/counties_checkpoint.csv"):
        print("Partial counties data exists.")
        county_df = pd.read_csv("./data/counties_checkpoint.csv", keep_default_na=False)
    else:
        print("No counties data exists.")
        county_df = place_df.assign(County="").reset_index(drop=True)

    # Loop through the counties dataframe to generate url skip if already exists
    count = 0
    base_place_url = "https://www.bestplaces.net/city/"
    for index, row in county_df.iterrows():
        place = row["Place"]
        state = row["State"]
        code = row["StateCode"]
        county = row["County"]

        # If the county has already been found, continue
        if county:
            continue

        # Identify and format the county name
        result = requests.get(f"{base_place_url}/{state}/{place}", verify=False)
        doc = BeautifulSoup(result.text, "html.parser")
        county = doc.find("b", text=re.compile(r'County:')).find_next_sibling().find("a").text
        county = "_".join(county.strip().split()[:-1]).lower()
        county_df.loc[index, "County"] = county
        print(f"Collected {place}, {code}.")

        # Sleep for around a second to keep from getting blacklisted
        time.sleep(random.uniform(0.5, 1))

        # Save to a csv every 100 counties
        if count % 50 == 0:
            county_df.to_csv("./data/counties_checkpoint.csv", index=False)

    # Save out the finalized data and delete the checkpoint file
    county_df.to_csv("./data/counties.csv", index=False, columns=["Places", "Counties"])
    os.remove("./data/counties_checkpoint.csv")

    return county_df


def get_cities(place_df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the Place identifier to a city name.

    Parameters
    ----------
    place_df : pd.DataFrame
        Dataframe with Place identifiers from BestPlaces.

    Returns
    -------
    city_df : pd.DataFrame
        Contains a column for the city name.
    """

    return city_df


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
    file_name = "geodata.csv"
    if os.path.isfile(f"./data/{file_name}"):
        print(f"The data for {file_name} has already been collected.\n")
        df = pd.read_csv(f"./data/{file_name}")
        return df
    print(f"Data {file_name} has not been generated yet: scraping data.")

   # File locations for the downloaded zip code data and its contents
    print("Downloading geographical data from Simplemaps.com.")
    url = "https://simplemaps.com//static/data/us-cities/1.75/basic/simplemaps_uscities_basicv1.75.zip"
    zip_loc = "data.zip"
    unpack_loc = "geodata_data"
    csv_file = "uscities.csv"
    urlretrieve(url, zip_loc)

    # Unpack the zip file and then delete the unused files
    shutil.unpack_archive(zip_loc, unpack_loc)
    shutil.move(f"{unpack_loc}/{csv_file}", f"./{csv_file}")
    shutil.move(zip_loc, f"{unpack_loc}/{zip_loc}")
    shutil.rmtree(unpack_loc)

    # Read in the CSV file and delete unused columns and incomplete rows
    raw_geodata_df = pd.read_csv(csv_file, index=False)

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
