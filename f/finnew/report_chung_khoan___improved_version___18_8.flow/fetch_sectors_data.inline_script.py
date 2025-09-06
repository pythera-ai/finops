# import wmill
import requests


def fetch_sectors_data(base_url: str):
    print(base_url)
    response = requests.get(f"{base_url}/top_sectors", timeout=30)
    print(response.content)
    response.raise_for_status()
    return response.json()


def sort_and_format_sectors(data, limit=5):
    if not data or "data" not in data:
        return []

    sorted_data = sorted(
        data["data"], key=lambda x: x.get("changePercent", 0), reverse=True
    )
    for item in sorted_data:
        item["changePercent"] = f"{item.get('changePercent', 0) * 100:.2f}%"
    return sorted_data[:limit]


def main(base_url: str = "http://172.18.0.10:8000"):
    try:
        data = fetch_sectors_data(base_url)
        print(data)
        processed_data = sort_and_format_sectors(data)
        return {
            "top_sectors": [e.get("icbName", "Unknown") for e in processed_data],
            "success": True,
            "data_quality": {
                "records_processed": len(processed_data),
                "source_records": len(data.get("data", [])) if data else 0,
            },
        }
    except Exception as e:
        print(f"Error fetching sectors data: {e}")
        return {"top_sectors": [], "success": False, "error": str(e)}
