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
    print("3. Clean drought data.")
    print("4. Get associated congressional districts.")
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
    # Clean and simplify download drought data
    drought_df = process.clean_drought()
    # Append congessional districts column
    base_df = collect.get_congressional_districts()

    print("\nEden terminated.")


if __name__ == "__main__":
    # Do something if this file is invoked on its own
    basic_pipline()
