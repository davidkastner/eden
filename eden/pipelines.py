"""Prebuilt pipelines using the Eden library."""

import eden.collect as collect


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

    city_df = collect.get_cities()
    geodata_df = collect.get_geodata()
    zips_df = collect.get_zips(city_df, geodata_df)

    return geodata_df


if __name__ == "__main__":
    # Do something if this file is invoked on its own
    basic_pipline()
