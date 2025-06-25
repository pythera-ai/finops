# import wmill

import requests


def main():
    base_url = "http://172.18.0.7:8000/index_summary"
    resp = requests.get(base_url)
    data = resp.json()["data"]

    result_dict = []

    key_dict_to_get = [
        "indexId",
        "indexValue",
        "change",
        "allQty",
        "allValue",
        "advances",
        "nochanges",
        "declines",
    ]
    for e in data:
        temp_dict = {}
        for key_need in key_dict_to_get:
            temp_dict[key_need] = e.get(key_need, "Không có")
        result_dict.append(temp_dict)
    return {"index_summary": result_dict}
