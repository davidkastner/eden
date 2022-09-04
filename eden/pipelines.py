"""Prebuilt pipelines using the Eden library."""

import eden.collect as collect
import eden.process as process


def basic_pipline() -> None:
    """
    Pipeline that collects all necessary data.

    The pipeline checks the data folder and skips collection if it exits.
    If you would like to update the data, clear the data folder.
    """

    print("\n.---------------.")
    print("| BASIC PIPLINE |")
    print(".---------------.\n")
    print("Outcomes:")
    print("1. Combined cities and states into a single dataframe.")
    print("2. Get geodata such as zipcode, latitude, population, etc.")
    print("-----------------\n")

    place_df = collect.get_places()
    raw_county_df = collect.get_counties(place_df)
    county_df = process.clean_counties(raw_county_df)
    raw_geodata_df = collect.download_geodata()
    city_df = process.places_to_cities(place_df)
    # geodata_df = collect.get_geodata(county_df, raw_geodata_df)
    # final_df = collect.merge_dataframes(place_df, city_df, county_df)

    print("\nEden terminated.")


if __name__ == "__main__":
    # Do something if this file is invoked on its own
    basic_pipline()
