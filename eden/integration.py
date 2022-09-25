"""Functions for integrating data flow across functions and modules."""

import pandas as pd
import os


def checkpoint_handler(function):
    """
    Determines the current state of the data and executes next step.

    Checks three states: 1) Has data collection not been started?
    2) Has it been started and a checkpoint file exists?
    3) Has data collection already been completed?

    Parameters
    ----------
    function : list[str]
       The function that needs to be run.

    Returns
    -------
    palce_df : pd.DataFrame
        Pandas dataframe with all Places.

    """
    # Look for county complete data, checkpoint, or no data
    if os.path.isfile("data/base.csv"):
        base_df = pd.read_csv("data/base.csv")
        if "County" in base_df:
            print("County data exists in Base.")
            county_df = base_df[["Place", "StateCode", "County"]]
            return county_df
    elif os.path.isfile("data/temp/county_checkpoint.csv"):
        print("Partial county data exists in checkpoint.")
        county_df = pd.read_csv(
            "data/temp/county_checkpoint.csv", keep_default_na=False
        )
    elif os.path.isfile("data/temp/county_raw.csv"):
        print("Raw county data exists.")
        county_df = pd.read_csv("data/temp/county_raw.csv")
        return county_df
    else:
        print("No county data exists.")
        county_df = place_df.assign(County="").reset_index(drop=True)
