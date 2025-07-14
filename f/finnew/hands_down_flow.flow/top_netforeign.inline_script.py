# import wmill
import requests


def get_top_10_sorted_tickers(data):
    sorted_buy = sorted(data["top_buy"], key=lambda x: x["value"], reverse=True)
    sorted_sell = sorted(
        data["top_sell"], key=lambda x: x["value"]
    )  # sell is negative, so ascending

    top_buy = [item["ticker"] for item in sorted_buy[:10]]
    top_sell = [item["ticker"] for item in sorted_sell[:10]]

    return {"top_netforeign": {"buy": top_buy, "sell": top_sell}}


def main():
    base_url = "http://172.18.0.10:8000/top_netforeign"
    resp = requests.get(base_url)
    data = resp.json()
    data = get_top_10_sorted_tickers(data)
    return data
