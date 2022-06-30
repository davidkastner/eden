"""Functions for data web scrapping."""

import os.path
import pickle
import pandas as pd
from bs4 import BeautifulSoup
import requests

# Necessary evil when using mac
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_states(states_csv: str = "./data/states.csv") -> tuple[list[str], list[str]]:
    """
    Get a list of state names and states codes from a CSV.

    Parameters
    ----------
    states_csv : str
        Location of the a csv with the states to analyze.

    Returns
    -------
    states_name : list[str]
        The names of all states in the current format.
    state_codes : list[str]
        The two letter codes for all the states.
    """

    # Create state name and code lists from from the state csv
    states_df = pd.read_csv(states_csv)
    states_dict = dict(states_df.values)
    state_names = list(states_dict.keys())
    state_codes = list(states_dict.values())

    return state_names, state_codes


def get_cities(state_names: list[str], state_codes: list[str]) -> dict[str : list[str]]:
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
    all_cities : dict[str: list[str]]
        State names as keys and all associate states as a list of strings.

    """

    # Check cities data exists and if it does retrieve it and early return
    file_name = "city_dict.pickle"
    if os.path.isfile(f"./data/{file_name}"):
        print("The city data for all states has already been collected.\n")
        all_cities_binary = open(f"./data/{file_name}", "rb")
        all_cities = pickle.load(all_cities_binary)
        return all_cities
    print("City data has not been generated yet: scraping data.\n")

    # Final dictionary with keys as states and all associated cities as values
    all_cities: dict[str : list[str]] = {}

    # The base url for searching for a states
    base_states_url = "https://www.bestplaces.net/find/state.aspx?state="

    # Loop through all state pages using the base url and each state code
    for index, state_code in enumerate(state_codes):
        print(f"Retrieving cities for {state_names[index]}.")
        city_list: list[str] = []
        result = requests.get(base_states_url + state_code, verify=False)
        doc = BeautifulSoup(result.text, "html.parser")

        # Select the div containing the city list and grab name from end of href
        cities_div = doc.find_all("div", class_="col-md-4")[0]
        cities = cities_div.find_all("a", href=True)
        for city in cities:
            city_url = city["href"]
            city = city_url.split("/")[-1]
            city_list.append(city)

        # Added cities list and state name as key value pair to final dictionary
        all_cities[state_names[index]] = city_list

    # Write the state name and city list and city name on a new line
    print(f"\nGenerating reuseable binary file for cities dictionary to a.\n")
    all_cities_binary = open(f"./data/{file_name}", "wb")
    pickle.dump(all_cities, all_cities_binary)
    all_cities_binary.close()

    return all_cities
