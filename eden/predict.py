"""Contains machine learing functionality."""

import pandas as pd
import sklearn
import os
import numpy as np


def drought_prediction() -> pd.DataFrame:
    """
    Predicts the risk of drought in 5 years for each county.

    Returns
    -------
    drought_df : pd.DataFrame
        Adds the drought data to the growing all.csv.
    """
    # Check data collection progress by whether a checkpoint file exists
    if os.path.isfile("data/temp/drought_predict_checkpoint.csv"):
        all_df = pd.read_csv("data/temp/drought_predict_checkpoint.csv")
    else:
        all_df = pd.read_csv("data/all.csv")
        all_df["DroughtRisk"] = np.nan

    # Open the cleaned timeseries drought data
    drought_df = pd.read_csv("data/temp/drought.csv")
    drought_df['MapDate'] = pd.to_datetime(drought_df['MapDate'])
    print(drought_df)


if __name__ == "__main__":
    # Don't forget to update the feature you want to plot
    drought_prediction()
