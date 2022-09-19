"""Contains machine learing functionality."""

import pandas as pd
from sklearn import linear_model
import os
import numpy as np
from datetime import date


def drought_prediction() -> pd.DataFrame:
    """
    Predicts the risk of drought in 5 years for each county.

    Returns
    -------
    drought_df : pd.DataFrame
        Adds the drought data to the growing all.csv.
    """
    # Create a Fips column to keep track of the counties
    drought_df = pd.read_csv("data/temp/drought.csv")
    fips = drought_df["FIPS"].unique()

    # Check data collection progress based on whether a checkpoint file exists
    if os.path.isfile("data/temp/drought_predict_checkpoint.csv"):
        drought_pred_df = pd.read_csv("data/temp/drought_predict_checkpoint.csv")
        completed = drought_pred_df.dropna()["Fips"].tolist()
        print("Drought prediction checkpoint file exists.")
    # Early return if the prediction data already exists
    elif os.path.isfile("data/temp/drought_predict.csv"):
        drought_pred_df = pd.read_csv("data/temp/drought_predict.csv")
        print("Drought prediction data exists.")
        return drought_pred_df
    # If the predictions have not been started create a new dataframe
    else:
        drought_pred_df = pd.DataFrame(fips, columns=["Fips"])
        drought_pred_df["Predict"] = np.nan
        completed = []
        print("No drought prediction data exists.")

    # Change to the data to the datetime pandas format
    drought_df['MapDate'] = drought_df['MapDate'].apply(lambda x: str(x)[:4]+"-"+str(x)[4:6]+"-"+str(x)[6:8])
    drought_df['MapDate'] = pd.to_datetime(drought_df['MapDate'])

    # Create a new column called Time representing the days since the start
    county_groups = drought_df.groupby("FIPS")
    prediction_count = 0
    for county, county_df in county_groups:
        if county in completed:
            continue
        first_date = drought_df['MapDate'].iat[-1]
        county_df['Days'] = drought_df['MapDate'].apply(lambda curr_date: (curr_date - first_date).days)

        # Build regression model and make a 5-year prediction
        reg = linear_model.LinearRegression()
        reg.fit(county_df[['Days']].values, county_df['Drought'].values)
        prediction = float(reg.predict([[10000]]))
        print(f"{prediction_count}. {county_df['County'].iat[0]} ({county}): {round(prediction, 3)}")

        # Store the prediction in the prediction df and save to checkpoint file
        drought_pred_df.loc[drought_pred_df["Fips"] == county, "Predict"] = prediction
        prediction_count += 1
        if prediction_count == 10:
            drought_pred_df.to_csv("data/temp/drought_predict_checkpoint.csv", index=False)
            prediction_count = 0

    drought_pred_df.to_csv("data/temp/drought_predict.csv", index=False)
    os.remove("data/temp/drought_predict_checkpoint.csv")


if __name__ == "__main__":
    # Don't forget to update the feature you want to plot
    drought_prediction()
