"""Functions for collecting geographical features for all cities in the US."""

import eden.process as process
import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
from urllib.request import urlretrieve
import shutil
import re
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

    # Look for county complete data, checkpoint, or no data
    base_df = pd.read_csv("data/base.csv")
    if "County" in base_df:
        print("County data exists.")
        county_df = base_df[["Place", "StateCode", "County"]]
        return county_df
    elif os.path.isfile("data/temp/counties_checkpoint.csv"):
        print("Partial counties data exists.")
        county_df = pd.read_csv(
            "data/temp/counties_checkpoint.csv", keep_default_na=False
        )
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
        county_parent = doc.find("b", text=re.compile(r"County:"))
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
        districts_df = pd.read_csv(
            "data/temp/districts_checkpoint.csv", keep_default_na=False
        )
    else:
        print("No districts data exists.")
        districts_df = base_df.assign(CongressionalDistrict="").reset_index(drop=True)

    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    for ind in base_df.index:
        lat = base_df["Latitude"][ind]
        long = base_df["Longitude"][ind]
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
        geodata_df = geodata_df[
            [
                "City",
                "StateCode",
                "Fips",
                "County",
                "Latitude",
                "Longitude",
                "Population",
                "Density",
                "Zip",
            ]
        ]
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
    # Check if the current main dataframe already contains the climate data
    all_df = pd.read_csv("data/all.csv")
    ft = ["HotScore", "ColdScore", "ClimateScore", "Rainfall", "Snowfall", "Precipitation",
          "Sunshine", "UV", "Elevation", "Above90", "Below30", "Below0"]
    if all([f in all_df for f in ft]):
        print("Climate data exists.")
        climate_df = pd.read_csv("data/all.csv", keep_default_na=False)
        return climate_df
    # Check if it is currently being collected (deleted when finished)
    elif os.path.isfile("data/temp/climate_checkpoint.csv"):
        print("Partial climate data exists.")
        climate_checkpoint_df = pd.read_csv(
            "data/temp/climate_checkpoint.csv", keep_default_na=False)
        climate_df = all_df.join(climate_checkpoint_df)
    # Data collection never started
    else:
        print("No climate data exists.")
        climate_df = base_df.assign(HotScore="", ColdScore="", ClimateScore="", Rainfall="", Snowfall="", Precipitation="",
                                    Sunshine="", UV="", Elevation="", Above90="", Below30="", Below0="").reset_index(drop=True)

    # Loop through the cities to generate URL, skip if already exists
    base_place_url = "https://www.bestplaces.net"
    state_dict = process.state_codes()
    for index, row in climate_df.iterrows():
        feature_list: list[str] = []
        place = row["Place"]
        code = row["StateCode"]
        state = state_dict[code]
        # If all features are already in the row continue without collecting
        if all([row[f] for f in ft]):
            continue
        # Retrieve web page as a BS4 object
        url = f"{base_place_url}/climate/city/{state}/{place}"
        result = requests.get(url, verify=False)
        doc = BeautifulSoup(result.text, "html.parser")

        # Get the climate scores
        score = doc.find("div", class_="display-4").string
        hot, cold = map(float, score.strip().split("/"))
        climate = round((hot + cold) / 2.0, 2)
        feature_list.extend([hot, cold, climate])

        # Get rainfall, snowfall, precipitation, sunshine, uv, and elevation
        table = doc.find_all('table')[0]
        for r in table.find_all('tr'):
            rows = r.find_all('td')[1].text.strip()
            feature_list.append(rows)
        # Remove rows containing unuseful features
        feature_list = [v for i, v in enumerate(feature_list) if i not in [3, 8, 9, 10]]

        # Get days above 90°, days below 30°, and days below 0°
        above90_tag = doc.find('h6', text=re.compile('over 90°')).text
        above90 = float(above90_tag.split(",")[1].split(" ")[3])
        below30_tag = doc.find('h6', text=re.compile('falls below freezing')).text
        below30 = float(below30_tag.split(",")[1].split(" ")[3])
        below0_tag = doc.find('h6', text=re.compile('falls below zero°')).text
        below0 = float(below0_tag.split(",")[1].split(" ")[3])
        feature_list.extend([above90, below30, below0])

        # Add the data to the dataframe
        climate_df.loc[index, ["HotScore", "ColdScore", "ClimateScore", "Rainfall", "Snowfall", "Precipitation",
                               "Sunshine", "UV", "Elevation", "Above90", "Below30", "Below0"]] = feature_list
        # Save the climate data to checkpoint file in case you lose connection
        columns = ["Place", "HotScore", "ColdScore", "ClimateScore", "Rainfall", "Snowfall",
                   "Precipitation", "Sunshine", "UV", "Elevation", "Above90", "Below30", "Below0"]
        climate_df.to_csv("./data/temp/climate_checkpoint.csv", index=False, columns=columns)
        print(f"Collected {place}, {code}")

    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")
    climate_df.to_csv("data/temp/climate.csv", index=False)
    os.remove("data/temp/climate_checkpoint.csv")

    return climate_df
