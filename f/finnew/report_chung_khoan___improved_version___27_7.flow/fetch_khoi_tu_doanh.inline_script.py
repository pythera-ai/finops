import requests


def fetch_khoi_tu_doanh(base_url: str):
    response = requests.get(
        f"{base_url}/khoi_tu_doanh", params={"period": "1W"}, timeout=30
    )
    response.raise_for_status()
    return response.json()


def main(stock_market: str, base_url: str = "http://172.18.0.10:8000"):
    KEY_FORWARD_COMPATIBLE = {"HSX": "VNINDEX", "HNX": "HNXINDEX", "UPCOM": "UPINDEX"}

    try:
        data = fetch_khoi_tu_doanh(base_url)
        print(data)
        if not data or "data" not in data:
            return {"khoi_tu_doanh": 0, "success": False, "error": "No data returned"}

        stock_market = stock_market.upper() if stock_market else "HSX"
        stock_market = KEY_FORWARD_COMPATIBLE.get(stock_market, stock_market)

        if stock_market not in data["data"]:
            print(f"Stock market {stock_market} not found in data")
            return {
                "khoi_tu_doanh": 0,
                "success": False,
                "error": f"Market {stock_market} not found",
            }

        market_data = data["data"][stock_market]
        if not market_data.get("data") or len(market_data["data"]) == 0:
            return {
                "khoi_tu_doanh": 0,
                "success": False,
                "error": "No market data available",
            }

        tu_doanh_value = market_data["data"][0].get("tuDoanh_MuaRong_Total", 0) / 10**9
        return {
            "khoi_tu_doanh": tu_doanh_value,
            "success": True,
            "data_quality": {
                "market_used": stock_market,
                "value_billions": tu_doanh_value,
            },
        }
    except Exception as e:
        print(f"Error fetching khoi tu doanh: {e}")
        return {"khoi_tu_doanh": 0, "success": False, "error": str(e)}
