from datetime import datetime, timedelta

import pandas as pd
from sklearn.linear_model import LinearRegression
from airflow.decorators import dag, task


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=['data_pipeline'],
)
def asset_pipeline():
    @task
    def processed_data():
        df = pd.read_csv("/tmp/data/sample_data.csv")

        df["age_group"] = pd.cut(
            df["age"],
            bins=[0, 30, 40, 100],
            labels=["Young", "Middle", "Senior"],
        )

        df.to_csv("/tmp/data/processed_data.csv", index=False)
        return "data loaded successfully"

    @task
    def country_populations():
        """Fetch and clean country population data."""
        df = pd.read_html("https://tinyurl.com/mry64ebh")[0]
        df.columns = ["country", "pop2022", "pop2023", "change", "continent", "region"]
        df["change"] = df["change"].str.rstrip("%").str.replace("−", "-").astype("float")
        return df.to_dict(orient="records")

    @task
    def continent_change_model(country_populations_data):
        df = pd.DataFrame(country_populations_data)
        data = df.dropna(subset=["change"])
        model = LinearRegression().fit(pd.get_dummies(data[["continent"]]), data["change"])
        return {
            "coef": model.coef_.tolist(),
            "intercept": model.intercept_,
            "feature_names": pd.get_dummies(data[["continent"]]).columns.tolist(),
        }

    @task
    def continent_stats(country_populations_data, model_data):
        df = pd.DataFrame(country_populations_data)
        result = df.groupby("continent").sum().reset_index()

        coefficients = dict(zip(model_data["feature_names"], model_data["coef"]))

        result["pop_change_factor"] = result["continent"].apply(
            lambda x: coefficients.get(f"continent_{x}", 0)
        )

        return result.to_dict(orient="records")

    cp_data = country_populations()
    model = continent_change_model(cp_data)
    processed_data_task = processed_data()
    stats = continent_stats(cp_data, model)

    [processed_data_task, stats]


asset_pipeline_dag = asset_pipeline()
