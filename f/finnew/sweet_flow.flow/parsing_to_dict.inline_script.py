import re
from datetime import datetime


def parse_change_value(change_str):
    """Parse change string like '↑ +4.63 ' or '↓ -2.34 '"""
    if not change_str:
        return 0.0, "unchanged"

    # Remove arrows and extract number, handle extra spaces
    clean_str = re.sub(r"[↑↓]", "", change_str).strip()
    try:
        value = float(clean_str.replace("+", ""))
        direction = (
            "up" if "↑" in change_str else "down" if "↓" in change_str else "unchanged"
        )
        return value, direction
    except:
        return 0.0, "unchanged"


def parse_cp_tang_giam(cp_str):
    """Parse CP tăng/giảm like '↑198|↔56|↓110'"""
    if not cp_str:
        return {"tang": 0, "khong_doi": 0, "giam": 0}

    try:
        # Extract numbers using regex
        tang_match = re.search(r"↑(\d+)", cp_str)
        khong_doi_match = re.search(r"↔(\d+)", cp_str)
        giam_match = re.search(r"↓(\d+)", cp_str)

        return {
            "tang": int(tang_match.group(1)) if tang_match else 0,
            "khong_doi": int(khong_doi_match.group(1)) if khong_doi_match else 0,
            "giam": int(giam_match.group(1)) if giam_match else 0,
        }
    except:
        return {"tang": 0, "khong_doi": 0, "giam": 0}


def parse_foreign_trading(value):
    """Parse foreign trading info like '↑ +2.29 triệu cp,↑ +71.16 tỷ đồng'"""
    parts = value.split(",")
    if len(parts) >= 2:
        cp_part = parts[0].strip()
        money_part = parts[1].strip()

        cp_value, cp_direction = parse_change_value(
            cp_part.replace("triệu cp", "").strip()
        )
        money_value, money_direction = parse_change_value(
            money_part.replace("tỷ đồng", "").strip()
        )

        return {
            "cp_trieu": cp_value,
            "cp_direction": cp_direction,
            "tien_ty_dong": money_value,
            "tien_direction": money_direction,
        }
    return {}


def main(download_result: dict):
    """Parse CSV content into structured data"""

    try:
        if not download_result or not download_result.get("success"):
            return {"success": False, "message": "Invalid CSV download result"}

        csv_content = download_result["csv_content"]
        lines = [line.strip() for line in csv_content.split("\n") if line.strip()]

        # Extract date from first line
        date_line = lines[0]
        date_match = re.search(r"(\d{2}/\d{2}/\d{4})", date_line)
        trading_date = (
            date_match.group(1) if date_match else datetime.now().strftime("%d/%m/%Y")
        )

        # Find header line and data start
        header_line_idx = -1
        for i, line in enumerate(lines):
            if "CHỈ SỐ" in line and "ĐIỂM" in line:
                header_line_idx = i
                break

        if header_line_idx == -1:
            raise ValueError("Could not find header line in CSV")

        # Parse market indices (from header line + 1 until empty line)
        market_indices = {}
        data_start = header_line_idx + 1

        for i in range(data_start, len(lines)):
            line = lines[i]
            if not line or line.startswith("Khối"):
                break

            if "," in line:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 7:  # Updated for new format with 7 columns
                    index_name = parts[0]
                    diem = float(parts[1])
                    thay_doi, huong = parse_change_value(parts[2])
                    phan_tram, _ = parse_change_value(parts[3])  # Extract percentage
                    klgd = float(parts[4])
                    gtgd = float(parts[5])
                    cp_tang_giam = parse_cp_tang_giam(parts[6])

                    market_indices[index_name] = {
                        "diem": diem,
                        "thay_doi": thay_doi,
                        "phan_tram": phan_tram,
                        "huong": huong,
                        "klgd_trieu_cp": klgd,
                        "gtgd_ty_dong": gtgd,
                        "cp_tang_giam": cp_tang_giam,
                    }

        # Parse other sections
        data_sections = {}

        for line in lines:
            if ":" in line and not line.startswith("CHỈ SỐ"):
                parts = line.split(":", 1)
                if len(parts) >= 2:
                    key = parts[0].strip()
                    value = parts[1].strip().strip(",").strip('"')

                    if key == "Khối ngoại":
                        data_sections["foreign_investors"] = parse_foreign_trading(
                            value
                        )

                    elif key == "Top mua ròng":
                        # Parse top buying stocks, remove quotes
                        stocks = [
                            stock.strip() for stock in value.replace('"', "").split(",")
                        ]
                        data_sections["top_net_buying"] = stocks

                    elif key == "Top bán ròng":
                        # Parse top selling stocks, remove quotes
                        stocks = [
                            stock.strip() for stock in value.replace('"', "").split(",")
                        ]
                        data_sections["top_net_selling"] = stocks

                    elif key == "Khối tự doanh":
                        # Parse proprietary trading
                        value_num, direction = parse_change_value(
                            value.replace("tỷ đồng", "").strip()
                        )
                        data_sections["proprietary_trading"] = {
                            "value_billion_vnd": value_num,
                            "direction": direction,
                        }

                    elif key == "Nhóm ngành nổi bật":
                        # Parse hot sectors, remove quotes
                        items = [
                            item.strip() for item in value.replace('"', "").split(",")
                        ]
                        data_sections["hot_sectors"] = items

                    elif key == "Cổ phiếu tâm điểm":
                        # Parse focus stocks, remove quotes
                        items = [
                            item.strip() for item in value.replace('"', "").split(",")
                        ]
                        data_sections["focus_stocks"] = items

                    elif "Tác động tăng" in key or "Tác động giảm" in key:
                        # Parse impact stocks with values
                        impact_match = re.search(r"\((.*?)\)", key)
                        impact_value = (
                            float(impact_match.group(1)) if impact_match else 0.0
                        )

                        stocks = [
                            stock.strip() for stock in value.replace('"', "").split(",")
                        ]
                        if "Tác động tăng" in key:
                            data_sections["positive_impact"] = {
                                "value": impact_value,
                                "stocks": stocks,
                            }
                        else:
                            data_sections["negative_impact"] = {
                                "value": impact_value,
                                "stocks": stocks,
                            }

        # Structure the final data
        structured_data = {
            "trading_date": trading_date,
            "market_indices": market_indices,
            "additional_info": data_sections,
            "vnindex_summary": market_indices.get("VNINDEX", {}),
            "top_stocks": {
                "net_buying": data_sections.get("top_net_buying", []),
                "net_selling": data_sections.get("top_net_selling", []),
                "focus_stocks": data_sections.get("focus_stocks", []),
            },
            "hot_sectors": data_sections.get("hot_sectors", []),
            "analysis": {
                "foreign_investors": data_sections.get("foreign_investors", {}),
                "proprietary_trading": data_sections.get("proprietary_trading", {}),
                "positive_impact": data_sections.get("positive_impact", {}),
                "negative_impact": data_sections.get("negative_impact", {}),
            },
        }

        return {
            "success": True,
            "data": structured_data,
            "raw_lines": lines,
            "message": "CSV data parsed successfully",
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to parse CSV data",
        }
