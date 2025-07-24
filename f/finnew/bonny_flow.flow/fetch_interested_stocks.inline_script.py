import requests


def fetch_interested_stocks(base_url: str):
    response = requests.get(f"{base_url}/top_interested_stocks", timeout=30)
    response.raise_for_status()
    return response.json()


def main(base_url: str = "http://172.18.0.10:8000"):
    try:
        data = fetch_interested_stocks(base_url)
        if not data or "data" not in data:
            return {"top_interested": [], "success": False, "error": "No data returned"}

        top_stocks = [e.get("symbol", "Unknown") for e in data["data"]][:10]
        return {
            "top_interested": top_stocks,
            "success": True,
            "data_quality": {
                "records_processed": len(top_stocks),
                "source_records": len(data["data"]),
            },
        }
    except Exception as e:
        print(f"Error fetching interested stocks: {e}")
        return {"top_interested": [], "success": False, "error": str(e)}
