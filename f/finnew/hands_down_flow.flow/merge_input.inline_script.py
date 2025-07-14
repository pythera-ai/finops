# import wmill


def merge_inputs(args):
    merged = {}

    for data in args:
        print("data: ", data)
        if not data:
            continue
        # data = json.loads(data)
        if "impact_up" in data:
            merged["impact_up"] = data["impact_up"]
        if "impact_down" in data:
            merged["impact_down"] = data["impact_down"]
        if "index_summary" in data:
            merged["index_summary"] = []
            for e in data["index_summary"]:
                e["allQty"] = e["allQty"] / 10**6
                e["allValue"] = e["allValue"] / 10**9
                merged["index_summary"].append(e)
        if "khoi_ngoai" in data:
            merged["khoi_ngoai"] = data["khoi_ngoai"]
        if "top_interested" in data:
            merged["top_interested"] = data["top_interested"]
        if "top_netforeign" in data:
            merged["top_netforeign"] = data["top_netforeign"]
        if "khoi_tu_doanh" in data:
            merged["khoi_tu_doanh"] = data["khoi_tu_doanh"]
        if "top_sectors" in data:
            merged["top_sectors"] = data["top_sectors"]
    return merged


def main(args: list):
    merged_data = merge_inputs(args)
    print(merged_data)
    return merged_data