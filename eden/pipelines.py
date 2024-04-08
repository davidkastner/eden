"""Prebuilt pipelines using the Eden library."""

import eden.collect as collect
import eden.process as process
import eden.predict as predict
import os

def basic_pipline() -> None:
    """
    Pipeline that collects all necessary data.

    The pipeline checks the data folder and skips collection if it exits.
    If you would like to update the data, clear the data folder.
    """

    print("\n.---------------.")
    print("| BASIC PIPELINE |")
    print(".---------------.\n")
    print("Outcomes:")
    print("1. Scrape identifying data for all cities and store it in base.csv.")
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
    process.add_house_voting_data()
    process.add_senate_voting_data()
    # predict.voting("RepVote")
    # predict.voting("DemVote")

    # Collect constitutionality data
    collect.get_districts_by_bioguide_ids()
    collect.get_percent_constitutionality()

    # Collect housing data
    collect.collect_housing_data()
    process.add_housing_data()
    
    # Pass in True to re-collect temple data
    collect.collect_temple_data()
    process.compute_temple_distances()

    # Collect and process crime data
    crime_df = collect.get_crime()
    process.clean_crime(crime_df, print_coverage=False)

    # Calculate the scores for each city
    predict.find_eden()

    print("\nEden terminated.")


if __name__ == "__main__":
    # Python version of bash's cd $(realpath $(dirname $0))
    # Necessary because the process.py and collect.py functions use relative paths
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Do something if this file is invoked on its own
    basic_pipline()
