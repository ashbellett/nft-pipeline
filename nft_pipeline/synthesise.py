import json
import os

import pandas as pd

from ingest import get_token_collection


def get_schemas() -> dict:

    with open("./data/config.json", mode="r", encoding="utf-8") as file:
        config = json.load(file)

    schemas = config["schemas"]

    return schemas


def read_responses(dataset: str, name: str) -> pd.DataFrame:

    json_list = []
    for file_name in os.listdir(f'./data/{name}/{dataset}/'):
        with open(f'./data/{name}/{dataset}/{file_name}') as file:
            json_list += json.load(file)

    data = pd.DataFrame(json_list)

    return data


def persist_data(data: pd.DataFrame, collection: str, label: str):

    data.to_parquet(
        f'./data/{collection}/{label}.parquet',
        engine="pyarrow"
    )
    # data.to_csv(
    #     f'./data/{collection}/{label}.csv',
    #     index=False
    # )


def clean_data():

    collections = get_token_collection()
    datasets = get_schemas().keys()

    for collection in collections:

        name = collection["name"]

        for dataset in datasets:

            data = read_responses(dataset, name)
            persist_data(data, name, dataset)


if __name__ == "__main__":

    clean_data()
