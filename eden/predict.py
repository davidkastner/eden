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
    

def voting(party) -> pd.DataFrame:
    """
    Predicts the voting outcomes for each city.

    """
    csv_name = "voting"
    voting_df = pd.read_csv(f"data/{csv_name}.csv")

    # Does a checkpoint file exist
    if os.path.isfile(f"data/temp/{csv_name}_predict_checkpoint.csv"):
        voting_pred_df = pd.read_csv(f"data/temp/{csv_name}_predict_checkpoint.csv")
        completed = voting_pred_df.dropna()["Place"].tolist()
        print("Voting prediction checkpoint file exists.")
    # Early return if the data exists
    elif os.path.isfile(f"data/temp/{csv_name}_predict.csv"):
        voting_pred_df = pd.read_csv(f"data/temp/{csv_name}_predict.csv")
        print("Voting prediction data exists.")
        return voting_pred_df
    # If unstarted, create a new dataframe
    else:
        voting_pred_df = voting_df.drop_duplicates(subset = ['Place','StateCode'], keep ='last').reset_index(drop=True)
        voting_pred_df = voting_pred_df[["Place", "StateCode", party]]
        voting_pred_df[f"{party}Pred"] = np.nan
        completed = []
        print("No voting prediction data exists.")

    # voting_pred_df only contains cities in rows not dates like voting_df
    # Change to dates to the datetime pandas format
    voting_df['Date'] = pd.to_datetime(voting_df['Date'])

    # Create a new column called Time representing the days since the start
    city_groups = voting_df.groupby(["Place", "StateCode"])
    prediction_count = 0
    for city, city_df in city_groups:
        if city in completed:
            continue
        first_date = voting_df['Date'].iat[-1]
        city_df['Days'] = voting_df['Date'].apply(lambda curr_date: (curr_date - first_date).days)

        # Sometime cities contain a "?" if the voting page was broken
        if "?" in city_df[party].values:
            continue
        else:
            # Build regression model and make a 4-year prediction
            reg = linear_model.LinearRegression()
            reg.fit(city_df[['Days']].values, city_df[party].values)
            prediction = float(reg.predict([[1460]]))
            print(f"{prediction_count}. {city[0]}, ({city[1]}): {round(prediction, 3)}")

        # Store the prediction in the prediction df and save to checkpoint file
        voting_pred_df.loc[(voting_pred_df['Place'] == city[0]) & (voting_pred_df['StateCode'] == city[1]), f"{party}Pred"] = prediction
        prediction_count += 1
        if prediction_count == 10:
            voting_pred_df.to_csv(f"data/temp/{csv_name}_predict_checkpoint.csv", index=False)
            prediction_count = 0

    voting_pred_df.to_csv(f"data/temp/{csv_name}_predict.csv", index=False)

    # Merge the results into all.csv
    all_df = pd.read_csv("data/all_test.csv")
    all_df = pd.merge(all_df,  voting_pred_df, on=["Place", "StateCode"])
    all_df.to_csv("data/all_test.csv", index=False)



def find_eden():
    """
    Normalizes all the features and then assigns an Eden Score to each city.

    """
    all_df = pd.read_csv("data/all_test.csv")

    # List of features that will be used in the Eden model
    features = ["Physicians", "HealthCosts", "WaterQuality", "AirQuality", "HotScore", "ClimateScore",
                "ColdScore", "Rainfall", "Snowfall", "Sunshine", "UV", "Above90", "Elevation",
                "Below30", "Below0","Density", "HouseConstitutionality", "SenateConstitutionality",
                "HomeInsurance", "Drought", "DemVotePred", "RepVotePred", "MedianHomeAge", "PropertyTaxRate", "MedianHomeCost"
                ]
    predict_df = all_df.filter(features)
    
    # Normalize the data
    normalize = lambda x: (x-x.min()) / (x.max()-x.min())
    predict_df = predict_df.apply(normalize)
 
    # The Eden Function - Negative value indicate unfavorable features
    eden = lambda x: round(x.Physicians*(.25)
                         - x.HealthCosts*(.25) 
                         + x.WaterQuality*(1) 
                         + x.AirQuality*(1) 
                         + x.Elevation*(2)
                         + x.ColdScore*(1) 
                         + x.HotScore*(3) 
                         - x.Above90*(1.5) 
                         - x.Below0*(1.5) 
                         + x.Rainfall*(1) 
                         - x.Snowfall*(5) 
                         - x.Drought*(3)
                         + x.Sunshine*(3) 
                         - x.Density*(1) 
                         - x.HomeInsurance*(2) 
                         + x.HouseConstitutionality*(.5) 
                         + x.SenateConstitutionality*(.5) 
                         - x.DemVotePred*(.5)
                         + x.RepVotePred*(.5)
                         - x.MedianHomeAge*(.5)
                         - x.PropertyTaxRate*(1.5)
                         - x.MedianHomeCost*(1.5), 3)

    predict_df["EdenScore"] = predict_df.apply(eden, axis = 1)
    
    # Add prediction to all.csv and write out
    all_df["EdenScore"] = predict_df["EdenScore"].values
    all_df.to_csv("data/all.csv", index=False)
    predict_df.to_csv("data/predict.csv", index=False)

if __name__ == "__main__":
    # Don't forget to update the feature you want to plot
    find_eden()
