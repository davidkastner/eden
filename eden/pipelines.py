"""Prebuilt pipelines using the Eden library."""

import eden.collect


def basic_pipline() -> None:
    """
    Pipeline that collects all necessary data.

    The pipeline checks the data folder and skips collection if it exits.
    If you would like to update the data, clear the data folder.
    """

    print("\n.---------------.")
    print("| BASIC PIPLINE |")
    print(".---------------.\n")
    print("Objectives:")
    print("1. Get states that we would like to analyze.")
    print("2. Scrape all cities for each state with formatting.")
    print("-----------------\n")

    state_names, state_codes = collect.get_states()
    cities_dict, cities_df = collect.get_cities(state_names, state_codes)

    return cities_dict, cities_df


if __name__ == "__main__":
    # Do something if this file is invoked on its own
    basic_pipline()
