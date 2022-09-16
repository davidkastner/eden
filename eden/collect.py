"""Functions for collecting geographical features for all cities in the US."""

import json
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
    # Won't exists yet if downloaded from Github
    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")
    # Check if Place data already exists
    if os.path.isfile("data/base.csv"):
        base_df = pd.read_csv("data/base.csv")
        if "Place" in base_df:
            place_df = pd.read_csv("data/base.csv", keep_default_na=False)
            place_df = place_df[["Place", "StateCode"]]
            print("Place data exists in Base.")
            return place_df
    elif os.path.isfile("data/temp/places.csv"):
        place_df = pd.read_csv("data/temp/places.csv", keep_default_na=False)
        print("Place data already exists in Places.")
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

    # This converts the list of lists to a csv with place and state columns
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
    if os.path.isfile("data/base.csv"):
        base_df = pd.read_csv("data/base.csv")
        if "County" in base_df:
            print("County data exists in Base.")
            county_df = base_df[["Place", "StateCode", "County"]]
            return county_df
    elif os.path.isfile("data/temp/county_checkpoint.csv"):
        print("Partial county data exists in checkpoint.")
        county_df = pd.read_csv(
            "data/temp/county_checkpoint.csv", keep_default_na=False
        )
    elif os.path.isfile("data/temp/county_raw.csv"):
        print("Raw county data exists.")
        county_df = pd.read_csv("data/temp/county_raw.csv")
        return county_df
    else:
        print("No county data exists.")
        county_df = place_df.assign(County="").reset_index(drop=True)

    # Loop through the county dataframe to generate url skip if already exists
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
        county_df.to_csv("./data/temp/county_checkpoint.csv", index=False)

    # Save out the finalized data and delete the checkpoint file
    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    # After the data has been collected write to csv and delete the checkpoint
    county_df.to_csv("data/temp/county_raw.csv", index=False)
    os.remove("data/temp/county_checkpoint.csv")

    return county_df


def get_congressional_districts() -> pd.DataFrame:
    """
    Retrieves congressional districts from 2020 and appends them to the base dataframe.

    This function retrieves the data and adds it to the final df.

    Returns
    -------
    districts_df : pd.DataFrame
        The base dataframe with the appended congressional district data.
    """
    if os.path.isfile("data/base.csv"):
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
        districts_df = base_df if "CongressionalDistrict" in base_df else base_df.assign(CongressionalDistrict="").reset_index(drop=True)

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
    districts_df.to_csv("data/base.csv", index=False)

    return districts_df


def get_districts_by_bioguide_ids() -> pd.DataFrame:
    congresses = [112, 113, 114, 115, 116, 117]
    sessions = ["1", "2"]
    parties = ["republican", "democrat"]
    # TODO extract this csv logic into a function to be used everwhere possibly
    csv_name = "bioguide_district_info"

    if os.path.isfile(f"data/{csv_name}.csv"):
        print("Districts data exists.")
        df = pd.read_csv(f"data/{csv_name}.csv", keep_default_na=False)

        return df
    elif os.path.isfile(f"data/temp/{csv_name}_checkpoint.csv"):
        print("Partial districts data exists.")
        df = pd.read_csv(f"data/temp/{csv_name}_checkpoint.csv", keep_default_na=False)
    else:
        print("No districts data exists.")
        df = pd.DataFrame(columns=["BioguideIds"] + congresses)

    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    for congress in congresses:
        for session in sessions:
            for party in parties:
                payload = json.dumps({
                    "congress": congress,
                    "session": session,
                    "branch": "house",
                    "party": party
                })

                print(payload)

                response = requests.request("POST", "https://www.freedomfirstsociety.org/wp-admin/admin-ajax.php?action=scorecard_query_bills", headers={
                    'Content-Type': 'application/json'
                }, data=payload).json()

                for voter in response["votes"]:
                    bioguide_id = voter["voter_meta"]["bioguide_id"]

                    name = voter["voter_meta"]["name"]
                    state = voter["voter_meta"]["state"]

                    if bioguide_id in df["BioguideIds"].values:
                        continue

                    representative_url = f'https://www.congress.gov/member/{name}/{bioguide_id}'
                    result = requests.get(representative_url, verify=False)
                    representative_html = BeautifulSoup(result.text, "html.parser").find(
                        "div", {"class": "overview-member-column-profile"}).findAll("th", {"class": "member_chamber"})
                    congress_info = {"BioguideIds": bioguide_id, 112: "", 113: "", 114: "", 115: "", 116: "", 117: ""}

                    for member_chamber in representative_html:
                        district_text = member_chamber.findNext("td").getText()
                        district_text_pieces = district_text.split()
                        term_congresses_text = district_text_pieces[-2]
                        term_congresses = re.findall(r'\d+', term_congresses_text)
                        term_congresses = [int(c) for c in term_congresses]

                        if len(term_congresses) > 1:
                            term_congresses = list(range(term_congresses[0], term_congresses[1]+1))

                        if "District At Large" in district_text or "District" not in district_text:
                            district = f'{state}-00'
                        else:
                            district_no = int(district_text.split("District ")[1][0])
                            district = f'{state}-0{district_no}' if district_no < 10 else f'{state}-{district_no}'

                        for term_congress in term_congresses:
                            if term_congress not in congresses:
                                continue
                            congress_info[term_congress] = district

                    df_dictionary = pd.DataFrame([congress_info])
                    df = pd.concat([df, df_dictionary], ignore_index=True)
                    print(congress_info)
                    df.to_csv(f"data/temp/{csv_name}_checkpoint.csv", index=False)

    # After the data has been collected write to csv and delete the checkpoint
    df.to_csv(f"data/{csv_name}.csv", index=False)
    os.remove(f"data/temp/{csv_name}_checkpoint.csv")


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
    if os.path.isfile("data/base.csv"):
        base_df = pd.read_csv("data/base.csv")
        if "Fips" in base_df:
            print("Geodata data exists in Base data.")
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
    elif os.path.isfile("data/temp/geodata_raw.csv"):
        print("Raw geodata exists.")
        geodata_df = pd.read_csv("data/temp/geodata_raw.csv")
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
    Scrapes climate data for all place IDs.

    Specifically scapes the normalized hot and cold scores, rainfall (in.),
    snowfall (in.), precipitation (days), sunshine (days), UV score, 
    elevation (ft.), days above 90°, days below 30°, and days below 0°

    Parameters
    ----------
    base_df : pd.DataFrame
        Extended dataframe with Place identifiers from BestPlaces.

    Returns
    -------
    climate_df : pd.DataFrame
        Base dataframe with all key city identifiers.
    """
    # Check if the current main dataframe already contains the climate data
    if os.path.isfile("data/climate.csv"):
        print("Climate data exists.")
        climate_df = pd.read_csv("data/climate.csv")
        return climate_df
    # Check if it is currently being collected (deleted when finished)
    elif os.path.isfile("data/temp/climate_checkpoint.csv"):
        print("Partial climate data exists.")
        climate_df = pd.read_csv("data/temp/climate_checkpoint.csv", keep_default_na=False)
    # Data collection never started
    else:
        print("No climate data exists.")
        base_df = pd.read_csv("data/base.csv")
        base_df = base_df[["Place", "StateCode"]]
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
        ft = ["HotScore", "ColdScore", "ClimateScore", "Rainfall", "Snowfall",
              "Precipitation", "Sunshine", "UV", "Elevation", "Above90", "Below30", "Below0"]
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
        climate_df.loc[index, ft] = feature_list

        # Save df to checkpoint every 50 cities in case you lose connection
        climate_df.to_csv("./data/temp/climate_checkpoint.csv", index=False)
        print(f"Collected {place}, {code}")
        time.sleep(float(random.uniform(0, 2)))

    climate_df.to_csv("data/climate.csv", index=False)
    # os.remove("data/temp/climate_checkpoint.csv")

    return climate_df


def get_health(base_df: pd.DataFrame) -> pd.DataFrame:
    """
    Scrapes the health data for all place IDs.

    Specifically collects physicians per capita, health costs index, 
    water quality index, air quality index, and commute times (min.).

    Parameters
    ----------
    base_df : pd.DataFrame
        Base dataframe with all key city identifiers.

    Returns
    -------
    health_df : pd.DataFrame
        Dataframe with raw health scores.
    """
    # Check if the current main dataframe already contains the climate data
    if os.path.isfile("data/health.csv"):
        print("Health data exists.")
        health_df = pd.read_csv("data/health.csv")
        return health_df
    # Check if it is currently being collected (deleted when finished)
    elif os.path.isfile("data/temp/health_checkpoint.csv"):
        print("Partial health data exists.")
        health_df = pd.read_csv("data/temp/health_checkpoint.csv", keep_default_na=False)
    # Data collection never started
    else:
        print("No health data exists.")
        base_df = pd.read_csv("data/base.csv")
        base_df = base_df[["Place", "StateCode"]]
        health_df = base_df.assign(Physicians="", HealthCosts="", WaterQuality="",
                                   AirQuality="").reset_index(drop=True)

    # Loop through the cities to generate URL, skip if already exists
    base_place_url = "https://www.bestplaces.net"
    state_dict = process.state_codes()
    for index, row in health_df.iterrows():
        feature_list: list[str] = []
        place = row["Place"]
        code = row["StateCode"]
        state = state_dict[code]
        ft = ["Physicians", "HealthCosts", "WaterQuality", "AirQuality"]
        # If all features are already in the row continue without collecting
        if all([row[f] for f in ft]):
            continue
        # Retrieve web page as a BS4 object
        url = f"{base_place_url}/health/city/{state}/{place}"
        result = requests.get(url, verify=False)
        doc = BeautifulSoup(result.text, "html.parser")

        # Get the health cost index, water quality, and air quality
        health = doc.find_all("div", class_="display-4")
        try:
            healthcost = float(health[0].get_text().replace(" ", "").split("/")[0])
        except:
            healthcost = "?"
        try:
            waterquality = float(health[1].get_text().replace(" ", "").split("/")[0])
        except:
            waterquality = "?"
        try:
            airquality = float(health[3].get_text().replace(" ", "").split("/")[0])
        except:
            airquality = "?"

        # Get the number of physicians per 10,000 people
        try:
            physicians_text = BeautifulSoup(result.text, "html.parser").find(text=re.compile(r'physicians per'))
            physicians = physicians_text.split(" ")[2]
        except:
            physicians = "?"

        # Add to the feature list and then the dataframe
        feature_list.extend([physicians, healthcost, waterquality, airquality])
        health_df.loc[index, ft] = feature_list

        # Save df to checkpoint every 50 cities in case you lose connection
        health_df.to_csv("./data/temp/health_checkpoint.csv", index=False)
        print(f"Collected {place}, {code}")
        time.sleep(float(random.uniform(0, 2)))

    health_df.to_csv("data/health.csv", index=False)

    return health_df
