"""Functions for geographic web scrapping."""

import os.path
import pickle
import pandas as pd
from bs4 import BeautifulSoup
import requests

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
    cities_df : pd.DataFrame
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
        cities_div = doc.find_all("div", class_="col-md-4")[0]
        cities = cities_div.find_all("a", href=True)
        for city in cities:
            city_url = city["href"]
            city = city_url.split("/")[-1]
            city_lol.append([city, state_names[index], state_code])

    # This converts the dictionary to a csv with city and state columns
    cities_df = pd.DataFrame(city_lol, columns=["City", "State", "StateCodes"])
    cities_df.to_csv("./data/cities.csv", index=False)

    return cities_df
