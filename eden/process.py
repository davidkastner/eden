"Functions for processing and formating collected data."

import pandas as pd
import os
import re
import numpy as np


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


def clean_drought():
    """
    Calculates standardized drought metric from raw droughtmonitor.unl.edu data.

    Returns
    -------
    drought_df : pd.DataFrame
        Standardized drought data combined metric normalized.
    """
    # Check if standardized drought data exists
    if os.path.isfile("data/drought.csv"):
        print(f"Standardized drought data exists.")
        df = pd.read_csv("data/drought.csv")
        return df

    print("Standardizing drought data.")
    drought_df = pd.read_csv("data/temp/drought_raw.csv")
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
    # Min-max normalize the resulting metrics
    drought_df["Drought"] = (drought_df["Drought"] - drought_df["Drought"].min()) / (
        drought_df["Drought"].max() - drought_df["Drought"].min()
    )

    drought_df.to_csv("data/drought.csv", index=False)

    return drought_df


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
