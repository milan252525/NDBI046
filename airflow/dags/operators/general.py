import os
import shutil

import pandas as pd
import requests


def download_file(url: str, name: str):
    response = requests.get(url, verify=False, timeout=300)
    if not os.path.exists("./tmp"):
        os.makedirs("./tmp")

    with open(os.path.join("./tmp", name), "wb") as file:
        file.write(response.content)


def cleanup(**kwargs):
    shutil.rmtree("./tmp")


def edit_enum(enum_path: str, cp_path: str):
    regions = pd.read_csv(cp_path)
    enum = pd.read_csv(enum_path)

    new_enum = pd.DataFrame(
        columns=["LAU", "NUTS", "CountyName", "RegionCode", "RegionName"]
    )

    code_map = {}
    for _, row in (
        regions[["KrajCode", "OkresCode", "Kraj", "Okres"]]
        .drop_duplicates()
        .dropna()
        .iterrows()
    ):
        code_map[row["OkresCode"]] = (row["Okres"], row["KrajCode"], row["Kraj"])

    for i, row in enum.drop_duplicates().dropna().iterrows():
        lau = row["CHODNOTA2"]
        nuts = row["CHODNOTA1"]

        try:
            new_enum.loc[i] = [lau, nuts, *code_map[nuts]]
        except KeyError:
            continue

    new_enum.to_csv(enum_path)
