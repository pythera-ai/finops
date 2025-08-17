import requests


def fetch_netforeign_data(base_url: str):
    response = requests.get(f"{base_url}/top_netforeign", timeout=30)
    response.raise_for_status()
    return response.json()


def process_netforeign_data(data, limit=10):
    if not data or "top_buy" not in data or "top_sell" not in data:
        return {"top_netforeign": {"buy": [], "sell": []}}

    sorted_buy = sorted(data["top_buy"], key=lambda x: x.get("value", 0), reverse=True)
    sorted_sell = sorted(data["top_sell"], key=lambda x: x.get("value", 0))

    return {
        "top_netforeign": {
            "buy": [item.get("ticker", "Unknown") for item in sorted_buy[:limit]],
            "sell": [item.get("ticker", "Unknown") for item in sorted_sell[:limit]],
        }
    }


def main(base_url: str = "http://172.18.0.10:8000"):
    try:
        data = fetch_netforeign_data(base_url)
        processed_data = process_netforeign_data(data)
        return {
            **processed_data,
            "success": True,
            "data_quality": {
                "buy_records": len(processed_data["top_netforeign"]["buy"]),
                "sell_records": len(processed_data["top_netforeign"]["sell"]),
            },
        }
    except Exception as e:
        print(f"Error fetching netforeign data: {e}")
        return {
            "top_netforeign": {"buy": [], "sell": []},
            "success": False,
            "error": str(e),
        }
