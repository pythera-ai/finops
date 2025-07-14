# import wmill

import requests

KEY_FORWARD_COMPATIBLE = {"HSX": "VNINDEX", "HNX": "HNXINDEX", "UPCOM": "UPINDEX"}


def main(stock_market: str):
    base_url = "http://172.18.0.10:8000/khoi_tu_doanh"
    resp = requests.get(base_url)
    data = resp.json()["data"]

    #
    if not stock_market:
        stock_market = "hnx"
    stock_market = stock_market.upper()
    stock_market = KEY_FORWARD_COMPATIBLE.get(stock_market, stock_market)
    if stock_market not in data.keys():
        return {}
    return {
        "khoi_tu_doanh": data[stock_market]["data"][0]["tuDoanh_MuaRong_Total"] / 10**9
    }
