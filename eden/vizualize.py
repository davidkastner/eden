"""Plot Coropleth maps -> update get_feature_profile to get started."""

import pandas as pd
import plotly.express as px
import os
from urllib.request import urlopen
import json


def save_feature_profile(feature):
    """
    A place to save parameters for features we have already set up.

    Use this to save your parameters so you want have to remember them.
    Add them to the feature_profiles dictionary.
    Once they have been added you are set, 
    just update the feature you want to plot at the end of vizualize.py.

    Parameters
    ----------
    feature : str
        The column name of the feature you would like to color the plot.

    """
    # Create a new key value pair for your feature -> Feature: [title, units, min, max]
    feature_profiles = {"Density": ["Population", "people/mile²", 50, 800],
                        "ClimateScore": ["Climate", "normalized", .25, .75],
                        "Rainfall": ["Rainfall", "in./year", 10, 80],
                        "Snowfall": ["Snowfall", "in./year", 0, 50],
                        "Precipitation": ["Raining", "days/year", 15, 140],
                        "Sunshine": ["Sunshine", "days/year", 150, 290],
                        "UV": ["UV Damage", "normalized", 3, 8],
                        "Elevation": ["Elevation", "miles", 0, 7000],
                        "Above90": ["Above 90°", "days", 0, 140],
                        "Below30": ["Below 30°", "days", 0, 220],
                        "Below0": ["Below 0°", "days", 0, 35],
                        "Physicians": ["Physicians", " per 10,000 persons", 0, 450],
                        "HealthCosts": ["Healthcare Cost", "normalized", .25, .9],
                        "WaterQuality": ["Water Quality", "normalized", .15, 1],
                        "AirQuality": ["Air Quality", "normalized", .35, 1],
                        "HouseConstitutionality": ["House Constitutionality", "averaged", 0, 1]
                        }

    # Get the parameters for you feature
    title = feature_profiles.get(feature)[0]
    units = feature_profiles.get(feature)[1]
    min = feature_profiles.get(feature)[2]
    max = feature_profiles.get(feature)[3]

    return title, units, min, max


def get_choropleth_map(feature, bounds: str = "Fips", csv: str = "all.csv") -> None:
    """
    Creates a choropleth map of the US by a geographical feature using Plotly.

    The function needs to know the "feature" you would like plotted,
    and whether you want to plot by county ("Fips") or state.
    Currently only works by county.
    By default, it will look in all.csv for the feature but that can be changed.
    The min value is assumed to be zero.

    Parameters
    ----------
    feature : str
        The column name of the feature you would like to color the plot.
    units : str
        The units that you would like to appear in the plot.
    min : str
        The smallest value.
    max : str
        The max value assigned on the map. Not likely the true max.
        It should be a high value but not too high if the data is "skewed" or "fattailed".

    """
    # Simple usage reminder to user
    print("\n.------------------.")
    print("| Coropleth Mapper |")
    print(".------------------.\n")
    print("Friendly neighborhood reminders:")
    print("1. First save your feature settings in save_feature_profile().")
    print("   The feature should exactly match a column header.")
    print("2. Update your feature/column name under __name__.")
    print("3. Run python vizualize.py")
    print("4. Enjoy your coropleth map!\n")

    # Import the json that matches the counties to fips data
    with urlopen(
        "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
    ) as response:
        counties = json.load(response)
    df = pd.read_csv(f"data/{csv}")

    # Remove rows that are missing data for the feature you are plotting
    df = df[df[feature].notna()]

    # Retrieve the mapping profile from your feature of interest
    title, units, min, max = save_feature_profile(feature)

    # Set up county boundaries
    if bounds == "County":
        bounds = "Fips"
    df = df[[feature, bounds, "County", "StateCode"]]
    # The leading zeros have been lost for four digit fips
    df["Fips"] = df["Fips"].apply(lambda x: "0" + str(x) if x < 10000 else x)
    # Average multiple cities that belong to the same county
    df = df.groupby([bounds, "County", "StateCode"])[feature].mean().reset_index()

    # Generate plot
    fig = px.choropleth(
        df,
        geojson=counties,
        locations="Fips",
        color=feature,
        color_continuous_scale="Viridis",
        range_color=(min, max),
        scope="usa",
        labels={feature: f"<b>{title} ({units})</b>"},
        hover_data={"County": True, "StateCode": True},
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    # Improve the legend
    fig.update_layout(
        coloraxis_colorbar=dict(
            thicknessmode="pixels",
            thickness=20,
            lenmode="pixels",
            len=500,
            yanchor="top",
            y=0.8,
        )
    )
    fig.show()

    fig.write_html(f"../docs/_static/{feature}.html")


if __name__ == "__main__":
    # Don't forget to update the feature you want to plot
    get_choropleth_map("HouseConstitutionality")
