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
    """
    Retrieves districts associated with bioguide ids by congress.

    This function retrieves the data and returns a new df.

    Returns
    -------
    df : pd.DataFrame
        The bioguide_district_info dataframe with bioguide ids and their districts.
    """
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
                            district_no = int(district_text_pieces[district_text_pieces.index('District') + 1])
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

def get_percent_constitutionality() -> pd.DataFrame:
    """
    Retrieves voting information and sorts it into years, districts, and states.

    This function retrieves the data and returns a new df.

    Returns
    -------
    df : pd.DataFrame
        The voting_info dataframe.
    """
    congress_to_years = {
        112: {
            "1": 2011,
            "2":2012
        },
        113: {
            "1":2013,
            "2": 2014
        },
        114: {
            "1":2015,
            "2": 2016
        },
        115: {
            "1": 2017,
            "2": 2018
        },
        116: {
            "1":2019,
            "2": 2020
        },
        117: {
            "1":2021,
            "2": 2022
        }
    }
    congresses = [112, 113, 114, 115, 116, 117]
    sessions = ["1", "2"]
    branches = ["house", "senate"]
    parties = ["republican", "democrat"]

    house_info = {2011: {}, 2012:{}, 2013: {}, 2014: {}, 2015:{}, 2016:{}, 2017:{}, 2018:{}, 2019:{}, 2020:{}, 2021:{}, 2022:{}}
    senate_info = {2011: {}, 2012:{}, 2013: {}, 2014: {}, 2015:{}, 2016:{}, 2017:{}, 2018:{}, 2019:{}, 2020:{}, 2021:{}, 2022:{}}
    bioguide_district_info = pd.read_csv(f"data/bioguide_district_info.csv", keep_default_na=False)

    for congress in congresses:
        for session in sessions:
            year = congress_to_years[congress][session]
            for branch in branches:
                for party in parties:
                    payload = json.dumps({
                        "congress": congress,
                        "session": session,
                        "branch": branch,
                        "party": party
                    })

                    print(payload)

                    response = requests.request("POST", "https://www.freedomfirstsociety.org/wp-admin/admin-ajax.php?action=scorecard_query_bills", headers={
                        'Content-Type': 'application/json'
                    }, data=payload).json()

                    for voter in response["votes"]:
                        bioguide_id = voter["voter_meta"]["bioguide_id"]
                        state = voter["voter_meta"]["state"]

                        if branch == "senate":
                            if state not in senate_info[year]:
                                senate_info[year][state] = {"correct_votes": 0, "incorrect_votes": 0, "total_votes": 0, "constitutional": 0}

                            for bill_no, bill_info in response["bills"].items():
                                if (bill_no not in voter):
                                    continue      
                                senate_info[year][state]["total_votes"] += 1

                                if bill_info["correct_vote"] == voter[bill_no]:
                                    senate_info[year][state]["correct_votes"] += 1
                                else:
                                    senate_info[year][state]["incorrect_votes"] += 1
                                senate_info[year][state]["constitutional"] =  round(senate_info[year][state]["correct_votes"] / senate_info[year][state]["total_votes"], 2)
                                
                            continue

                        district = bioguide_district_info.loc[bioguide_district_info['BioguideIds'] == bioguide_id][str(congress)].iloc[0]

                        if not district:
                            print(f"District doesn't exist for {bioguide_id} during congress {congress}")

                            continue

                        if district not in house_info[year]:
                            house_info[year][district] = {"state": state, "correct_votes": 0, "incorrect_votes": 0, "total_votes": 0, "constitutional": 0}

                        for bill_no, bill_info in response["bills"].items():  
                            if (bill_no not in voter):
                                continue
                            house_info[year][district]["total_votes"] += 1

                            if bill_info["correct_vote"] == voter[bill_no]:
                                house_info[year][district]["correct_votes"] += 1
                            else:
                                house_info[year][district]["incorrect_votes"] += 1
                            house_info[year][district]["constitutional"] =  round(house_info[year][district]["correct_votes"] / house_info[year][district]["total_votes"], 2)

    df = pd.DataFrame(columns=["CongressionalDistrict", "Branch", "Year", "Constitutional (0-1)", "State"])
    
    for year in range(2011, 2023):
        for district in house_info[year]:
            state = house_info[year][district]["state"]
            constitutional = house_info[year][district]["constitutional"]
            voting_info = {"CongressionalDistrict": district, "Branch": "house", "Year": year, "Constitutional (0-1)": constitutional, "State": state}
            df_dictionary = pd.DataFrame([voting_info])
            df = pd.concat([df, df_dictionary], ignore_index=True)
        for state in senate_info[year]:
            constitutional = senate_info[year][state]["constitutional"]
            voting_info = {"CongressionalDistrict": "N\A", "Branch": "senate", "Year": year, "Constitutional (0-1)": constitutional, "State": state}
            df_dictionary = pd.DataFrame([voting_info])
            df = pd.concat([df, df_dictionary], ignore_index=True)

    df.to_csv(f"data/constitutional_voting_info.csv", index=False)

    return df

def collect_voting_data():
    """
    Scrapes the voting data for all place IDs.

    Specifically collects percentage republican vs democrat by voting year.

    Returns
    -------
    voting_df : pd.DataFrame
        Dataframe with voting data percentages by year.
    """
    csv_name = "voting"

    if os.path.isfile(f"data/{csv_name}.csv"):
        print("Districts data exists.")
        df = pd.read_csv(f"data/{csv_name}.csv", keep_default_na=False)

        return df
    elif os.path.isfile(f"data/temp/{csv_name}_checkpoint.csv"):
        print("Partial voting data exists.")
        df = pd.read_csv(f"data/temp/{csv_name}_checkpoint.csv", keep_default_na=False)
    else:
        print("No voting data exists.")
        df = pd.DataFrame(columns=["Date", "Place", "StateCode", "RepVote", "DemVote"])


    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    base_df = pd.read_csv("data/base.csv")
    base_df = base_df[["Place", "StateCode"]]
    base_place_url = "https://www.bestplaces.net"
    state_dict = process.state_codes()
    df_last_row = df.iloc[-1]
    start = False

    for index, row in base_df.iterrows():
        place = row["Place"]
        code = row["StateCode"]
        state = state_dict[code]
        default_timeline = [2000, 2004, 2008, 2012, 2016, 2020]

        if df_last_row["Place"] == place and df_last_row["StateCode"] == code and df_last_row["Date"] == "2020-01-01":
            start = True
            
            continue

        if not start:
            continue

        url = f"{base_place_url}/voting/city/{state}/{place}"
        result = requests.get(url, verify=False)
        html = BeautifulSoup(result.text, "html.parser").findAll(
            "div", {"class": "card-body m-0 p-0"})

        try:
            chart_javscript = html[2].find("script").text
            lists = [l.strip("][\"").split(",") for l in re.findall(r'\[.*?\]', chart_javscript)]
            timeline, democrat, republican, _ = lists
            timeline = [int(year.strip("\' ")) for year in timeline]
            democrat = [float(percentage) for percentage in democrat]
            republican = [float(percentage) for percentage in republican]
        except:
            democrat = ["?", "?", "?", "?", "?", "?"]
            republican = ["?", "?", "?", "?", "?", "?"]
            timeline = default_timeline

        voting_data = []

        for index, year in enumerate(timeline):
            voting_info = {
                "Date":f"{year}-01-01", "Place":place, "StateCode":code, "RepVote":republican[index], "DemVote":democrat[index]
            }
            voting_data.append(voting_info)

        df_dictionary = pd.DataFrame(voting_data)
        df = pd.concat([df, df_dictionary], ignore_index=True)

        time.sleep(float(random.uniform(0, 2)))

        df.to_csv(f"data/temp/{csv_name}_checkpoint.csv", index=False)

    df.to_csv(f"data/{csv_name}.csv", index=False)

    return df

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
