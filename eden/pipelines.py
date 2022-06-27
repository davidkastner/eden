"""Prebuilt pipelines using the Eden library."""

import collect


def basic_pipline() -> None:
    """
    Full data collection pipeline.

    """
    print("\n.---------------.")
    print("| BASIC PIPLINE |")
    print(".---------------.\n")
    print("Objectives:")
    print("1. Get states that we would like to analyze.")
    print("2. Scrape all cities for each state with formatting.")
    print("-----------------\n")

    state_names, state_codes = collect.get_states()
    all_cities = collect.get_cities(state_names, state_codes)


if __name__ == "__main__":
    # Do something if this file is invoked on its own
    basic_pipline()
