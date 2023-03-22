"Functions for processing and formating collected data."

import pandas as pd
import os
import re
import numpy as np
from scipy import spatial
from geopy.distance import geodesic


def clean_counties(raw_county_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert county names to a consistent format.

    Parameters
    ----------
    raw_counties_df : pd.DataFrame
        The raw counties names from BestPlaces.

    Returns
    -------
    counties_df : pd.DataFrame
        Cleaned county names.
    """

    # If no city was found delete the row
    county_df = raw_county_df[raw_county_df.County != "?"].copy(deep=True)
    # Remove county from the end, if it ocurred
    county_df["County"] = county_df["County"].apply(
        lambda x: x.rsplit(" ", 1)[0] if "county" in x else x
    )
    # If there is perentheses at the end remove them
    county_df["County"] = county_df["County"].apply(
        lambda x: re.sub(r" ?\([^)]+\)", "", x)
    )
    county_df["County"] = county_df["County"].apply(lambda x: x.strip("_"))
    # Remove spaces and replace with underscores
    county_df["County"] = county_df["County"].apply(lambda x: x.replace(" ", "_"))
    # Save the new county data and rewrite the old data
    print("Saving the cleaned counties data.")

    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    county_df.to_csv("data/temp/county_clean.csv", index=False)

    return county_df


def places_to_cities(place_df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the Place identifiers to city names.

    Parameters
    ----------
    place_df : pd.DataFrame
        Dataframe with Place identifiers from BestPlaces.

    Returns
    -------
    city_df : pd.DataFrame
        Contains a column for the city name.
    """
    temp = "data/temp"
    if os.path.isfile(f"{temp}/cities.csv"):
        print(f"Cities data exists.")
        df = pd.read_csv(f"{temp}/cities.csv")
        return df
    print("No cities data exists.")

    # Make a copy and update the column title
    city_df = place_df.copy(deep=True)
    city_df = city_df.rename(columns={"Place": "City"})
    # Remove paranthesis with county data
    city_df["City"] = city_df["City"].apply(lambda x: re.sub(r" ?\([^)]+\)", "", x))
    city_df["City"] = city_df["City"].apply(lambda x: x.strip("_"))
    # Remove county data after a dash
    city_df["City"] = city_df["City"].apply(
        lambda x: x.split("-")[0] if x.split("_")[-1] == "county" and "-" in x else x
    )

    if not os.path.exists("data/temp"):
        os.mkdir("data/temp")

    city_df.to_csv("data/temp/cities.csv", index=False)

    return city_df


def clean_geodata(raw_geodata_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and process the raw geodata.

    Parameters
    ----------
    raw_geodata_df : pd.DataFrame
        The raw downloaded geodata.

    Returns
    -------
    geodata_df : pd.DataFrame
        Geodata with city, state, fip, county, lat, long, pop, density, zip.
    """
    # If data has already been collected in base.csv use that instead
    if os.path.isfile("data/base.csv"):
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
    elif os.path.isfile("data/temp/geodata_raw.csv"):
        geodata_df = pd.read_csv("data/temp/geodata_raw.csv")

        # Clean up raw geodata
    print("Cleaning geographical data.")
    columns_to_drop = [
        "city_ascii",
        "state_name",
        "source",
        "military",
        "incorporated",
        "timezone",
        "ranking",
        "id",
    ]
    geodata_df = raw_geodata_df.drop(columns_to_drop, axis=1)
    geodata_df = geodata_df.dropna()

    # Update column names
    geodata_df.columns = [
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

    # Correct formating for the City, State and County names
    columns_to_format = ["City", "County", "StateCode"]
    for column in columns_to_format:
        geodata_df[column] = geodata_df[column].str.lower()
        geodata_df[column] = geodata_df[column].str.replace(" ", "_")

    geodata_df.to_csv("./data/temp/geodata_clean.csv", index=False)

    return geodata_df


def geodata_intersect(
    county_df: pd.DataFrame, city_df: pd.DataFrame, geodata_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Identifies intersection the city, county, and geodata.

    Parameters
    ----------
    county_df : pd.DataFrame
        The growing dataframe with the cities, states, statecodes, and counties.
    city_df : pd.DataFrame
        Contains a column for the city name.
    geodata_df : pd.DataFrame
        The cleaned and processed geodata.

    Returns
    -------
    base_df : pd.DataFrame
        Merged data from with places, city, county, and geodata.
    """
    # Check if the base dataframe has already been created
    if os.path.isfile("data/base.csv"):
        print("Base dataframe already exists.")
        base_df = pd.read_csv("data/base.csv")
        return base_df
    else:
        print("Generating base dataframe.")

    # Create it if it hasn't been created
    city_col = city_df["City"]
    combined_df = county_df.join(city_col)
    reordered_df = combined_df[["Place", "City", "County", "StateCode"]]

    # Clean the BestPlaces county data
    headers = ["City", "County"]
    remove_ending = [
        "_census_area",
        "_city_and_borough",
        "_borough",
        "_cdp",
        "_municipality"]
    for header in headers:
        for ending in remove_ending:
            reordered_df[header] = reordered_df[header].apply(
                lambda x: x.split(ending)[0]
            )

    # Create a new dataframe with only the cities in common to remove errors
    base_df = pd.merge(reordered_df, geodata_df, on=["City", "StateCode", "County"])
    base_df.to_csv("data/base.csv", index=False)

    # Use to vizualize the columns that failed to merge
    failed_df = reordered_df.merge(
        geodata_df, indicator=True, on=["City", "StateCode", "County"], how="left"
    ).loc[lambda x: x["_merge"] != "both"]
    failed_df.to_csv("data/temp/dropped.csv", index=False)

    return base_df


def clean_climate(raw_climate_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes units and normalizes the scrapped climate data.

    Returns
    -------
    all_df : pd.DataFrame
        Adds the climate data to the growing all.csv.
    """
    # Check if the climate data has already been added to all.csv
    if os.path.isfile("data/all.csv"):
        all_df = pd.read_csv("data/all.csv")
        if "ClimateScore" in all_df:
            print("Climate data exists.")
            return all_df
    climate_df = raw_climate_df

    # Replace question marks with NaN
    climate_df.replace('?', np.nan)

    # Clean climate features
    features = ["HotScore", "ColdScore", "ClimateScore", "Rainfall", "Snowfall",
                "Precipitation", "Sunshine", "UV", "Elevation", "Above90", "Below30", "Below0"]
    normalize = ["HotScore", "ColdScore", "ClimateScore"]
    for feature in features:
        # Remove the period at the end of some columns and all non-alphanumeric characters
        climate_df[feature] = climate_df[feature].apply(lambda x: float(
            re.sub(r'[^0-9.]', '', str(x).strip("."))) if str(x)[-1] == "." else float(re.sub(r'[^0-9.]', '', str(x))))
        if feature in normalize:
            climate_df[feature] = round((climate_df[feature]-climate_df[feature].min()) /
                                        (climate_df[feature].max()-climate_df[feature].min()), 3)

    # Merge the combined data with all.csv
    all_df = pd.merge(climate_df, all_df, on=["Place", "StateCode"])
    all_df.to_csv("data/all.csv", index=False)
    print("Climate data added to all.csv")

    return climate_df


def add_house_voting_data():
    """
    Removes units and normalizes the scrapped health data.

    Returns
    -------
    all_df : pd.DataFrame
        Adds the house averaged voting data to the growing all.csv.
    """
    voting_info = pd.read_csv(f"data/constitutional_voting_info.csv", keep_default_na=False)
    voting_info = voting_info.loc[voting_info['Branch'] == "house"][["CongressionalDistrict", "Constitutional (0-1)"]]
    voting_info = voting_info.groupby(["CongressionalDistrict"])["Constitutional (0-1)"].mean().reset_index()
    voting_info.rename(columns={'Constitutional (0-1)': 'HouseConstitutionality'}, inplace=True)
    all_df = pd.read_csv("data/all.csv")
    all_df = pd.merge(all_df, voting_info, on=["CongressionalDistrict"])
    all_df.to_csv("data/all.csv", index=False)

    return voting_info


def add_senate_voting_data():
    """
    Removes units and normalizes the scrapped health data.

    Returns
    -------
    all_df : pd.DataFrame
        Adds the senate averaged voting data to the growing all.csv.
    """
    voting_info = pd.read_csv(f"data/constitutional_voting_info.csv", keep_default_na=False)
    voting_info = voting_info.loc[voting_info['Branch'] == "senate"][["State", "Constitutional (0-1)"]]
    voting_info['State'] = voting_info['State'].str.lower()
    voting_info = voting_info.groupby(["State"])["Constitutional (0-1)"].mean().reset_index()
    voting_info.rename(columns={'Constitutional (0-1)': 'SenateConstitutionality'}, inplace=True)
    voting_info.rename(columns={'State': 'StateCode'}, inplace=True)
    all_df = pd.read_csv("data/all.csv")
    all_df = pd.merge(all_df, voting_info, on=["StateCode"])
    all_df.to_csv("data/all.csv", index=False)

    return voting_info


def combine_house_and_senate_data():
    """
    Combines house and senate data into all.csv.

    Returns
    -------
    all_df : pd.DataFrame
        Adds the senate and house averaged voting data to the growing all.csv.
    """
    all_df = pd.read_csv("data/all.csv")
    all_df['Constitutionality'] = (all_df['SenateConstitutionality'] + all_df['HouseConstitutionality']) * 2 / 3
    all_df.to_csv("data/all.csv", index=False)

    return all_df

def compute_temple_distances():
    """
    Gets the distance of each place from the nearest temple in miles.

    Returns
    -------
    all_df : pd.DataFrame
        Adds temple distances to the growing all.csv.
    """
    temples_df = pd.read_csv("data/temples.csv")
    all_df = pd.read_csv("data/all.csv")
    temple_coords = list(zip(temples_df.Latitude, temples_df.Longitude))

    def get_distance(place):
        tree = spatial.KDTree(temple_coords)
        nearest_coord = tree.query([place])
        temple_index = nearest_coord[1][0]
        nearest_temple = temple_coords[temple_index]
        return round(geodesic(place, nearest_temple).mi)

    all_df['TempleDistance'] = all_df.apply(lambda x:
                                            get_distance((x['Latitude'], x['Longitude'])),
                                            axis=1
                                        )

    all_df.to_csv("data/all.csv", index=False)

    return all_df

def add_housing_data():
    """
    Combines house and senate data into all.csv.

    Returns
    -------
    all_df : pd.DataFrame
        Adds the senate and house averaged voting data to the growing all.csv.
    """
    housing_info = pd.read_csv(f"data/housing.csv", keep_default_na=False)
    housing_info.rename(columns={
        'Median Home Age': 'MedianHomeAge',
        'Median Home Cost' : "MedianHomeCost",
        'Property Tax Rate': "PropertyTaxRate"
    }, inplace=True)
    housing_info = housing_info[["MedianHomeAge", "PropertyTaxRate", "MedianHomeCost", "Place", "StateCode"]]
    all_df = pd.read_csv("data/all.csv")
    all_df = pd.merge(housing_info, all_df, on=["Place", "StateCode"])
    all_df.to_csv("data/all_test.csv", index=False)

    return all_df


def clean_health(raw_health_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes units and normalizes the scrapped health data.

    Returns
    -------
    all_df : pd.DataFrame
        Adds the health data to the growing all.csv.
    """
    # Check if the health data has already been added to all.csv
    if os.path.isfile("data/all.csv"):
        all_df = pd.read_csv("data/all.csv")
        if "Physicians" in all_df:
            print("Health data exists.")
            return all_df
    health_df = raw_health_df

    # Replace question marks with NaN
    health_df = health_df.replace('?', np.nan)
    # health_df = health_df.dropna()
    # health_df.to_csv("data/health_error.csv", index=False)

    # Clean health features
    features = ["Physicians", "HealthCosts", "WaterQuality", "AirQuality"]
    normalize = ["WaterQuality", "AirQuality"]
    reverse_normalize = ["HealthCosts"]
    for feature in features:
        # Remove the period at the end of some columns and all non-alphanumeric characters
        health_df[feature] = health_df[feature].apply(lambda x: x.replace(",", "") if isinstance(x, str) else x)
        health_df[feature] = health_df[feature].apply(lambda x: x if x == "?" else float(x))
        if feature in normalize:
            health_df[feature] = round((health_df[feature]-health_df[feature].min()) /
                                       (health_df[feature].max()-health_df[feature].min()), 3)
        if feature in reverse_normalize:
            health_df[feature] = round(abs((health_df[feature]-health_df[feature].min()) /
                                       (health_df[feature].max()-health_df[feature].min()) - 1), 3)

    # Merge the combined data with all.csv
    all_df = pd.merge(health_df, all_df, on=["Place", "StateCode"])
    all_df.to_csv("data/all.csv", index=False)
    print("Health data added to all.csv")

    return health_df


def merge_home_insurance() -> pd.DataFrame:
    """
    Merges the house insurance data.

    Returns
    -------
    all_df : pd.DataFrame
        Adds the house insurance data to the growing all.csv.
    """
    # Check whether a data collection is in progress
    if os.path.isfile("data/all_insurance.csv"):
        all_df = pd.read_csv("data/all_insurance.csv")
    elif os.path.isfile("data/all.csv"):
        all_df = pd.read_csv("data/all.csv")
        if "HomeInsurance" in all_df:
            print("HomeInsurance data exists in all.csv.")
            return all_df
    else:
        all_df = pd.read_csv("data/all.csv")
        all_df["HomeInsurance"] = np.nan
    homes_df = pd.read_csv("data/temp/home_insurance.csv")

    # Loop over the zip codes in all.csv
    for index_all, zips_all in all_df.iterrows():
        prices: list[int] = []
        # Check if a value has already been computed for the current city
        if isinstance(all_df.at[index_all, "HomeInsurance"], int):
            continue

        # The zips codes are strings so we convert them to integars
        zips_list = zips_all["Zip"].split()
        zips_list = [int(z) for z in zips_list]

        # Loop over the zip codes in home_insurance.csv
        for index_home, zip_home in homes_df.iterrows():
            if zip_home["Zip"] in zips_list:
                prices.append(int(homes_df.iloc[index_home]["Price"].replace("$", "").replace(",", "")))

        # Average the collected prices for each city
        if len(prices) == 0:
            continue
        else:
            average_price = sum(prices) / len(prices)
        all_df.at[index_all, "HomeInsurance"] = average_price

    all_df.to_csv("data/all_insurance.csv", index=False)
    print("Merged home insurance into all.csv")


def clean_drought():
    """
    Calculates standardized drought metric from raw droughtmonitor.unl.edu data.

    Returns
    -------
    drought_df : pd.DataFrame
        Standardized drought data combined metric normalized.
    """
    # Look for drought complete data, checkpoint, or no data
    if os.path.isfile("data/all.csv"):
        all_df = pd.read_csv("data/all.csv")
        if "Drought" in all_df:
            print("Drought data exists in all.csv.")
            return
    if os.path.isfile("data/temp/drought_raw.csv"):
        print("Raw drought data exists.")
        drought_df = pd.read_csv("data/temp/drought_raw.csv")
    else:
        return print("No drought data exists visit droughtmonitor.unl.edu.")

    print("Standardizing drought data.")
    # Change the name of the FIPS column to match the Fips column in all.csv
    drought_df.rename(columns={'FIPS': 'Fips'}, inplace=True)
    # Create a new column for the drought metric
    drought_df["Drought"] = 0
    # Drought metric is the severity multiplied by the affect population summed
    drought_df["Drought"] = drought_df.apply(
        lambda x: (x.D1) + (x.D2 * 2) + (x.D3 * 3) + (x.D4 * 4), axis=1
    )
    drought_df = drought_df.drop(
        [
            "ValidStart",
            "ValidEnd",
            "StatisticFormatID",
            "None",
            "D0",
            "D1",
            "D2",
            "D3",
            "D4",
        ],
        axis=1,
    )

    # Calculate the mean drought exposure for each county
    drought_df = drought_df.groupby(["Fips"])["Drought"].mean().reset_index()

    # Normalize the drought data between 0 and 1
    drought_df["Drought"] = round((drought_df["Drought"]-drought_df["Drought"].min()) /
                                  (drought_df["Drought"].max()-drought_df["Drought"].min()), 3)
    # Store the drought data
    all_df = pd.merge(all_df, drought_df, on=["Fips"])
    all_df.to_csv("data/all.csv", index=False)

    return


def state_codes() -> dict:
    """
    Retrieve a dictionary of state codes and names.

    Returns
    -------
    state_dict : dict[str:str]
        Dictionary with state clodes as keys and state names as values.
    """

    # Conversion dictionary for state codes and names
    state_dict = {
        "al": "alabama",
        "ak": "alaska",
        "az": "arizona",
        "ar": "arkansas",
        "ca": "california",
        "co": "colorado",
        "ct": "connecticut",
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
        "wy": "wyoming",
    }

    return state_dict

if __name__ == "__main__":
    # Don't forget to update the feature you want to plot
    add_housing_data()
