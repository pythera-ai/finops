import requests


def fetch_khoi_ngoai(base_url: str):
    response = requests.get(f"{base_url}/khoi_ngoai", params={"period":"1W"}, timeout=30)
    response.raise_for_status()
    return response.json()


def main(stock_market: str, base_url: str = "http://172.18.0.10:8000"):
    KEY_FORWARD_COMPATIBLE = {"HSX": "VNINDEX", "HNX": "HNXINDEX", "UPCOM": "UPINDEX"}

    try:
        data = fetch_khoi_ngoai(base_url)
        print(data)

        if not data or "data" not in data:
            return {
                "khoi_ngoai": {"vol": 0, "net_value": 0},
                "success": False,
                "error": "No data returned",
            }

        stock_market = stock_market.upper() if stock_market else "HSX"
        stock_market = KEY_FORWARD_COMPATIBLE.get(stock_market, stock_market)

        if stock_market not in data["data"]:
            print(f"Stock market {stock_market} not found")
            return {
                "khoi_ngoai": {"vol": 0, "net_value": 0},
                "success": False,
                "error": f"Market {stock_market} not found",
            }

        market_data = data["data"][stock_market].get("data", {})
        vol = market_data.get("tradingVolumeChart_first", {}).get("value", 0) / 10**6
        net_value = (
            market_data.get("tradingValueChart_first", {}).get("value", 0) / 10**9
        )

        return {
            "khoi_ngoai": {"vol": vol, "net_value": net_value},
            "success": True,
            "data_quality": {
                "market_used": stock_market,
                "volume_millions": vol,
                "value_billions": net_value,
            },
        }
    except Exception as e:
        print(f"Error fetching khoi ngoai: {e}")
        return {
            "khoi_ngoai": {"vol": 0, "net_value": 0},
            "success": False,
            "error": str(e),
        }
