"""Functions for geographic web scrapping."""

import os.path
import pickle
import pandas as pd
from bs4 import BeautifulSoup
import requests
from urllib.request import urlretrieve
import shutil

# Necessary evil when using mac
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_cities() -> pd.DataFrame:
    """
    Scrapes site for a list of all cities.

    For later scraping we need a complete list of all city names.

    Parameters
    ----------
    states_name : list[str]
        The names of all states in the current format.
    state_codes : list[str]
        The two letter codes for all the states.

    Returns
    -------
    city_df : pd.DataFrame
        Pandas dataframe with all cities

    """

    # Get the states names and two letter codes from reference
    state_names = list(pd.read_csv("./data/states.csv")["Name"])
    state_codes = list(pd.read_csv("./data/states.csv")["Code"])

    # Check cities data exists, if it does retrieve it and early return
    file_name = "cities.csv"
    if os.path.isfile(f"./data/{file_name}"):
        print("The city data for all states has already been collected.\n")
        cities_df = pd.read_csv(f"./data/{file_name}")
        return cities_df
    print("City data has not been generated yet: scraping data.\n")

    # Final dictionary with keys as states and all associated cities as values
    cities_dict: dict[str: list[str]] = {}

    # The base url for searching for a states
    base_states_url = "https://www.bestplaces.net/find/state.aspx?state="

    # List of lists for creating the final dataframe and csv file
    city_lol: list[list[str, str, str]] = []

    # Loop through all state pages using the base url and each state code
    for index, state_code in enumerate(state_codes):
        print(f"Retrieving cities for {state_names[index]}.")
        result = requests.get(base_states_url + state_code, verify=False)
        doc = BeautifulSoup(result.text, "html.parser")

        # Select the div containing the city list and grab name from end of href
        cities_div = doc.find_all("div", class_="col-md-4")
        for city_div in cities_div:
            cities = city_div.find_all("a", href=True)
            for city in cities:
                city_url = city["href"]
                city = city_url.split("/")[-1]
                city_lol.append([city, state_names[index], state_code])

    # This converts the dictionary to a csv with city and state columns
    city_df = pd.DataFrame(city_lol, columns=["City", "State", "StateCodes"])
    city_df.to_csv("./data/cities.csv", index=False)

    return city_df


def get_county(city_df: pd.DataFrame) -> pd.DataFrame:
    """
    Scrapes site for a list of all cities.

    Parameters
    ----------
    city_df : pd.DataFrame
        Pandas dataframe with all cities

    Returns
    -------
    zipcode_df : pd.DataFrame
        Pandas dataframe with all the zip codes associated with each city
    """

   # File locations for the downloaded zip code data and its contents
    url = "https://simplemaps.com/static/data/us-zips/1.80/basic/simplemaps_uszips_basicv1.80.zip"
    zip_loc = "data.zip"
    unpack_loc = "zip_data"
    csv_file = "uszips.csv"
    urlretrieve(url, zip_loc)

    # Unpack the zip file and then delete the unused files
    shutil.unpack_archive(zip_loc, unpack_loc)
    shutil.move(f"{unpack_loc}/{csv_file}", f"./{csv_file}")
    shutil.move(zip_loc, f"{unpack_loc}/{zip_loc}")
    shutil.rmtree(unpack_loc)

    # Read in the CSV file and delete unused columns
    zip_df = pd.read_csv(csv_file)
    columns_to_drop = ["zcta", "parent_zcta", "county_weights",
                       "county_names_all", "county_fips_all", "imprecise", "military", "timezone"]
    zip_df = zip_df.drop(columns_to_drop, axis=1)

    # Correct formating for the City, State and County names
    columns_to_format = ["city", "state_name", "county_name"]
    for column in columns_to_format:
        zip_df[column] = zip_df[column].str.lower()
        zip_df[column] = zip_df[column].str.replace(" ", "_")

    return
