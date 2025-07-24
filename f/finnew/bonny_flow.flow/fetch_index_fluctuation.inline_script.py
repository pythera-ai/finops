import requests


def fetch_index_fluctuation(base_url: str, index_name: str):
    response = requests.get(
        f"{base_url}/index_fluctuation", params={"index_name": index_name}, timeout=90
    )
    response.raise_for_status()
    return response.json()


def main(stock_market: str, base_url: str = "http://172.18.0.10:8000"):
    stock_market = stock_market if stock_market else "HSX"

    try:
        data = fetch_index_fluctuation(base_url, stock_market)

        if "error" in data:
            print(f"API Error: {data['error']}")
            return {
                "impact_up": {"stock_code": [], "total": 0},
                "impact_down": {"stock_code": [], "total": 0},
                "success": False,
                "error": data["error"],
            }

        if not data or "data" not in data:
            return {
                "impact_up": {"stock_code": [], "total": 0},
                "impact_down": {"stock_code": [], "total": 0},
                "success": False,
                "error": "No data returned",
            }

        index_increase = []
        index_decrease = []

        for item in data["data"]:
            impact = item.get("index_affect", 0)
            if impact >= 0:
                index_increase.append(item)
            else:
                index_decrease.append(item)

        return {
            "impact_up": {
                "stock_code": [e.get("ticker", "Unknown") for e in index_increase],
                "total": sum([e.get("index_affect", 0) for e in index_increase]),
            },
            "impact_down": {
                "stock_code": [e.get("ticker", "Unknown") for e in index_decrease],
                "total": sum([e.get("index_affect", 0) for e in index_decrease]),
            },
            "success": True,
            "data_quality": {
                "increase_count": len(index_increase),
                "decrease_count": len(index_decrease),
                "market_used": stock_market,
            },
        }
    except Exception as e:
        print(f"Error fetching index fluctuation: {e}")
        return {
            "impact_up": {"stock_code": [], "total": 0},
            "impact_down": {"stock_code": [], "total": 0},
            "success": False,
            "error": str(e),
        }
