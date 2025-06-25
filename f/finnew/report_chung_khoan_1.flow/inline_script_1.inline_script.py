# import wmill
import requests


def main():
    base_url = "http://172.18.0.7:8000/top_interested_stocks"
    resp = requests.get(base_url)
    data = resp.json()
    data = data["data"]
    top_stock = [e["symbol"] for e in data][:10]
    return {"top_interested": top_stock}
