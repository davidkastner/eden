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
    # Check if the current main dataframe already contains this datacolumn
    base_df = pd.read_csv("data/base.csv")
    if "Place" in base_df:
        print("Place data exists.")
        place_df = pd.read_csv("data/base.csv", keep_default_na=False)
        place_df = place_df[["Place", "StateCode"]]
        return place_df

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

    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    place_df.to_csv("data/temp/places.csv", index=False)

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
    base_df = pd.read_csv("data/base.csv")
    if "Counties" in base_df:
        print("Counties data exists.")
        county_df = pd.read_csv("data/base.csv", keep_default_na=False)
        county_df = county_df[["Place", "StateCode", "County"]]
        return county
    elif os.path.isfile("data/temp/counties_checkpoint.csv"):
        print("Partial counties data exists.")
        county_df = pd.read_csv("data/temp/counties_checkpoint.csv", keep_default_na=False)
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
    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    # After the data has been collected write to csv and delete the checkpoint
    county_df.to_csv("data/temp/counties_raw.csv", index=False)
    os.remove("data/temp/county_checkpoint.csv")

    return county_df


def get_congressional_districts() -> pd.DataFrame:
    """
    Retrieves congressional districts from 2020 and appends them to the base dataframe.

    This function retrieves the data and adds it to the final df.

    Returns
    -------
    districts_df : pd.DataFrame
        The base dataframe witht he appended congressional district data.
    """

    base_df = pd.read_csv("data/base.csv")

    if os.path.isfile("data/temp/districts_raw.csv"):
        print("Districts data exists.")
        districts_df = pd.read_csv("data/temp/districts_raw.csv", keep_default_na=False)
        districts_df.to_csv("data/base.csv", index=False)

        return districts_df
    elif os.path.isfile("data/temp/districts_checkpoint.csv"):
        print("Partial districts data exists.")
        districts_df = pd.read_csv("data/temp/districts_checkpoint.csv", keep_default_na=False)
    else:
        print("No districts data exists.")
        districts_df = base_df.assign(CongressionalDistrict="").reset_index(drop=True)

    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    for ind in base_df.index:
        lat = base_df['Latitude'][ind]
        long = base_df['Longitude'][ind]
        district = districts_df["CongressionalDistrict"][ind]

        if district:
            continue

        url = f"https://api.mapbox.com/v4/govtrack.cd-117-2020/tilequery/{long},{lat}.json?radius=0&access_token="

        result = requests.get(url, verify=False).json()
        state = result["features"][0]["properties"]["state"]
        district_no = result["features"][0]["properties"]["number"]
        district = f"{state}-{district_no}"
        districts_df.loc[ind, "CongressionalDistrict"] = district

        # Save the districts out to a checkpoint file
        print(f"Collected {ind}, {district}")

        districts_df.to_csv("./data/temp/districts_checkpoint.csv", index=False)

    # After the data has been collected write to csv and delete the checkpoint
    districts_df.to_csv("data/temp/districts_raw.csv", index=False)
    os.remove("data/temp/districts_checkpoint.csv")

    return districts_df


def download_geodata() -> pd.DataFrame:
    """
    Retrieves geographical data such as zip codes, county, and latitude.

    This function only retrieves the data and does not add it to the final df.

    Returns
    -------
    raw_geodata_df : pd.DataFrame
        The raw downloaded geodata.
    """

    # Look for geodata exists
    base_df = pd.read_csv("data/base.csv")
    if "Fips" in base_df:
        print("Geodata data exists.")
        geodata_df = pd.read_csv("data/base.csv", keep_default_na=False)
        geodata_df = geodata_df[["City", "StateCode", "Fips", "County",
                                 "Latitude", "Longitude", "Population", "Density", "Zip"]]
        return geodata_df

   # File locations for the downloaded zip code data and its contents
    print("Downloading geographical.")
    unpack_loc = "data/temp"
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


def get_climate(base_df: pd.DataFrame) -> pd.DataFrame:
    """
    Scrapes the climatescore data for all place IDs.

    This consists of a standardized high, low and overall scores.

    Parameters
    ----------
    base_df : pd.DataFrame
        Extended dataframe with Place identifiers from BestPlaces.

    Returns
    -------
    climatescore_df : pd.DataFrame
        Dataframe with raw climate scores.
    """

    # Check if the current main dataframe already contains this datacolumn
    all_df = pd.read_csv("data/all.csv")
    if "ClimateScore" in all_df:
        print("Climate data exists.")
        climate_df = pd.read_csv("data/all.csv", keep_default_na=False)
        return climate_df
    # Check if it is currently being collected (deleted when finished)
    elif os.path.isfile("data/temp/climate_checkpoint.csv"):
        print("Partial climate data exists.")
        climate_checkpoint_df = pd.read_csv("data/temp/climate_checkpoint.csv", keep_default_na=False)
        climate_df = all_df.join(climate_checkpoint_df)

    # Collection was never started or the file was deleted
    else:
        print("No climate data exists.")
        climate_df = base_df.assign(HotScore="", ColdScore="", ClimateScore="").reset_index(drop=True)

    # Loop through the places dataframe to generate URL skip if already exists
    base_place_url = "https://www.bestplaces.net"
    state_dict = process.state_codes()
    for index, row in climate_df.iterrows():
        place = row["Place"]
        code = row["StateCode"]
        state = state_dict[code]
        hotscore = row["HotScore"]
        coldscore = row["HotScore"]
        climatescore = row["ClimateScore"]

        # If all three climate scores have already been collected, skip row
        if hotscore and coldscore and climatescore:
            continue

        # Identify and format the county name
        url = f"{base_place_url}/climate/city/{state}/{place}"
        result = requests.get(url, verify=False)
        doc = BeautifulSoup(result.text, "html.parser")
        climate = doc.find("div", class_="display-4").string
        # Skip cities that return a 401 error and label with a "?"
        if climate != None:
            hot, cold = climate.strip().split("/")
            hotscore = float(hot)
            coldscore = float(cold)
            climatescore = round((hotscore + coldscore) / 2.0, 2)  # Hot and cold average
        else:
            climatescore = "?"

        # Add the collected values to the dataframe
        climate_df.loc[index, "HotScore"] = hotscore
        climate_df.loc[index, "ColdScore"] = coldscore
        climate_df.loc[index, "ClimateScore"] = climatescore

        # Save only the climate data to the checkpoint file
        print(f"Collected {place}, {code}")
        columns = ["HotScore", "ColdScore", "ClimateScore"]
        climate_df.to_csv("./data/temp/climate_checkpoint.csv", index=False, columns=columns)

    # Save out the finalized data and delete the checkpoint file
    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    # After the data has been collected write to csv and delete the checkpoint
    climate_df.to_csv("data/all_df.csv", index=False)
    os.remove("data/temp/climate_checkpoint.csv")

    return climate_df
