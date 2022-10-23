"""Prebuilt pipelines using the Eden library."""

import eden.collect as collect
import eden.process as process
import eden.predict as predict


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
    print("1. Scrape identifying data for all cities and stor it in base.csv.")
    print("1. Scrape additional raw geographical data for all cities.")
    print("2. Process the data and store the final features in all.csv.")
    print("-----------------\n")

    # Uses the non-ambiguous code names from BestPlaces
    place_df = collect.get_places()
    # County data from BestPlaces
    raw_county_df = collect.get_counties(place_df)
    county_df = process.clean_counties(raw_county_df)
    # Download the geodata with zip, pop, etc.
    raw_geodata_df = collect.download_geodata()
    # Convert Place identifiers to city names
    city_df = process.places_to_cities(place_df)
    # Clean the downloaded raw geodata
    geodata_df = process.clean_geodata(raw_geodata_df)
    # Generate base working df from the intersection of all dataframes
    base_df = process.geodata_intersect(county_df, city_df, geodata_df)
    # Append congessional districts column
    # collect.get_congressional_districts()
    # Collect the raw climate data
    raw_climate_df = collect.get_climate(base_df)
    # Collect the health climate data
    raw_health_df = collect.get_health(base_df)
    # Clean the climate data
    process.clean_climate(raw_climate_df)
    # Clean the health data and store the final data in all.csv
    process.clean_health(raw_health_df)
    # Clean the home insurance data and add it to all.csv
    process.merge_home_insurance()
    # Clean drought data
    process.clean_drought()
    # Collects presidential voting data by city
    collect.collect_voting_data()
    # Collect housing data
    collect.collect_housing_data()
    process.add_housing_data()

    # predict.voting("RepVote")
    # predict.voting("DemVote")

    # Calculate the scores for each city
    predict.find_eden()

    print("\nEden terminated.")


if __name__ == "__main__":
    # Do something if this file is invoked on its own
    basic_pipline()
