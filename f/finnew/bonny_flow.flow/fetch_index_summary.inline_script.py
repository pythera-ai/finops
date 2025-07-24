import requests


def fetch_index_summary(base_url: str):
    response = requests.get(f"{base_url}/index_summary", timeout=30)
    response.raise_for_status()
    return response.json()


def main(base_url: str = "http://172.18.0.10:8000"):
    try:
        data = fetch_index_summary(base_url)
        print(data)
        if not data or "data" not in data:
            return {"index_summary": [], "success": False, "error": "No data returned"}

        key_fields = [
            "indexId",
            "indexValue",
            "change",
            "changePercent",
            "allQty",
            "allValue",
            "advances",
            "nochanges",
            "declines",
        ]

        result_dict = []
        for item in data["data"]:
            temp_dict = {}
            for key in key_fields:
                temp_dict[key] = item.get(key, "N/A")
            result_dict.append(temp_dict)

        return {
            "index_summary": result_dict,
            "success": True,
            "data_quality": {
                "records_processed": len(result_dict),
                "source_records": len(data["data"]),
            },
        }
    except Exception as e:
        print(f"Error fetching index summary: {e}")
        return {"index_summary": [], "success": False, "error": str(e)}
