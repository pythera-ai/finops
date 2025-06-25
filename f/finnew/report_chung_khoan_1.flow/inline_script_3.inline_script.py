# import wmill

import requests


def main(stock_market: str):
    # boundary input
    if not stock_market:
        stock_market = "HSX"
    #
    base_url = "http://172.18.0.7:8000/index_fluctuation"
    resp = requests.get(base_url, params={"index_name": stock_market}).json()
    if "error" in resp.keys():
        print(resp["error"])
        return {}
    data = resp["data"]
    #
    index_increase = []
    index_decrease = []
    for e in data:
        if e["index_affect"] >= 0:
            index_increase.append(e)
        else:
            index_decrease.append(e)
    #
    return {
        "impact_up": {
            "stock_code": [e["ticker"] for e in index_increase],
            "total": sum([e["index_affect"] for e in index_increase]),
        },
        "impact_down": {
            "stock_code": [e["ticker"] for e in index_decrease],
            "total": sum([e["index_affect"] for e in index_decrease]),
        },
    }
