import pandas as pd
from sklearn.linear_model import LinearRegression

import dagster as dg


@dg.asset
def processed_data():
    df = pd.read_csv("data/sample_data.csv")

    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 30, 40, 100],
        labels=["Young", "Middle", "Senior"],
    )

    df.to_csv("data/processed_data.csv", index=False)
    return "data loaded succesfully"


@dg.asset
def country_populations() -> pd.DataFrame:
    df = pd.read_html("https://tinyurl.com/mry64ebh")[0]
    df.columns = ["country", "pop2022", "pop2023", "change", "continent", "region"]
    df["change"] = df["change"].str.rstrip("%").str.replace("−", "-").astype("float")
    return df


@dg.asset
def continent_change_model(country_populations: pd.DataFrame) -> LinearRegression:
    data = country_populations.dropna(subset=["change"])
    return LinearRegression().fit(pd.get_dummies(data[["continent"]]), data["change"])


@dg.asset
def continent_stats(
    country_populations: pd.DataFrame, continent_change_model: LinearRegression
) -> pd.DataFrame:
    result = country_populations.groupby("continent").sum()
    result["pop_change_factor"] = continent_change_model.coef_
    return result


defs = dg.Definitions(
    assets=[
        processed_data,
        country_populations,
        continent_change_model,
        continent_stats,
    ]
)
