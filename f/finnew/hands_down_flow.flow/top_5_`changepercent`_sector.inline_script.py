# import wmill
import requests


def sort_and_format_sectors(data):
    sorted_data = sorted(data, key=lambda x: x["changePercent"], reverse=True)
    for item in sorted_data:
        item["changePercent"] = f"{item['changePercent'] * 100:.2f}%"
    return sorted_data


def main():
    base_url = "http://172.18.0.10:8000/top_sectors"
    resp = requests.get(base_url)
    data = resp.json()["data"]
    data = sort_and_format_sectors(data)[:5]
    return {"top_sectors": [e["icbName"] for e in data]}
