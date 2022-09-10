"""Contains graphing and plotting functionality."""

import pandas as pd
import plotly.express as px
import os
from urllib.request import urlopen
import json


def get_choropleth_map(feature: str, bounds: str = "Fips", csv: str = "all.csv") -> None:
    """
    Creates a choropleth map of the US by a geographical feature using Plotly.

    The function needs to know the "feature" you would like plotted,
    and whether you want to plot by county or state.
    By default, it will look in all.csv for the feature but that can be changed.

    Parameters
    ----------
    feature : str
        The column name of the feature you would like to color the plot.
    county : str
        Merge the data by County or State.
    csv : str
        The file name containing the data to be plotted.

    """
    # Import the json that matches the counties to fips data
    with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
        counties = json.load(response)

    df = pd.read_csv(f"data/{csv}")
    # The user may enter county, which is more accurately expressed as Fip codes
    if bounds == "County":
        bounds = "Fips"
    df = df[[feature, bounds, "County", "StateCode"]]

    # The leading zeros have been lost for four digit fips
    df["Fips"] = df["Fips"].apply(lambda x: "0" + str(x) if x < 10000 else x)

    # Average duplicate Fips rows originating from multiple cities per county
    df = df.groupby([bounds, "County", "StateCode"])[feature].mean().reset_index()
    df.to_csv("choropleth.csv", index=False)

    # Generate plot
    max = 1500
    fig = px.choropleth(df,
                        geojson=counties,
                        locations='Fips',
                        color='Density',
                        color_continuous_scale="Viridis",
                        # Set max manually if the highest value is an outlier (e.g., pop. of New York City)
                        range_color=(0, max),
                        scope="usa",
                        labels={'Density': '<b>Population Density</b>'},
                        hover_data={"County": True, "StateCode": True})
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    # Improve the legend
    fig.update_layout(coloraxis_colorbar=dict(
        thicknessmode="pixels", thickness=20,
        lenmode="pixels", len=500,
        yanchor="top", y=0.8,
        ticksuffix=" People"
    ))
    fig.show()

    # Check if a plots folder exists, if not make one
    if not os.path.exists("data/plots"):
        os.mkdir("data/plots")
    fig.write_html("data/plots/density.html")


if __name__ == "__main__":
    # Do something if this file is invoked on its own
    get_choropleth_map("Density")
