# import wmill
import requests


def sort_and_format_sectors(data):
    sorted_data = sorted(data, key=lambda x: x["changePercent"], reverse=True)
    for item in sorted_data:
        item["changePercent"] = f"{item['changePercent'] * 100:.2f}%"
    return sorted_data


def main():
    base_url = "http://172.18.0.7:8000/top_sectors"
    resp = requests.get(base_url)
    data = resp.json()["data"]
    data = [e for e in data if e['changePercent'] > 0]
    data = sort_and_format_sectors(data)
    return {"top_sectors": [e['icbName'] for e in data]}
