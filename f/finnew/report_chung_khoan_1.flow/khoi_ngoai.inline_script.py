# import wmill


import requests

KEY_FORWARD_COMPATIBLE = {"HSX": "VNINDEX", "HNX": "HNXINDEX", "UPCOM": "UPINDEX"}


def main(stock_market: str):
    base_url = "http://172.18.0.7:8000/khoi_ngoai"
    resp = requests.get(base_url)
    data = resp.json()["data"]
    # HNX -> HNXINDEX
    stock_market = stock_market.upper()
    if not stock_market:
        print("Không chỉ định mã, mặc định là sàn HSX")
        stock_market = "HSX"
    stock_market = KEY_FORWARD_COMPATIBLE.get(stock_market, stock_market)

    if stock_market not in data.keys():
        print(f"Không có sàn chứng khoán: {stock_market}")
        return {}

    return {
        "khoi_ngoai": {
            "vol": data[stock_market]["data"]["tradingVolumeChart_first"]["value"]
            / 10**6,  # triệu cổ phiếu
            "net_value": data[stock_market]["data"]["tradingValueChart_first"]["value"]
            / 10**9,  # tỷ
        }
    }
