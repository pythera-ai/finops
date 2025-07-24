import wmill
from typing import Dict, Any, List


def main(market_data: Dict[str, Any], top_count: int = 3) -> Dict[str, Any]:
    """Trích xuất thông tin chỉ số thị trường tăng/giảm mạnh dựa trên percentage"""
    try:
        if not market_data.get("success"):
            raise ValueError("Invalid market data")
        data = market_data["data"]
        market_indices = data.get("market_indices", {})
        if not market_indices:
            raise ValueError("No market indices data found")
        all_indices = []
        for index_name, index_data in market_indices.items():
            percentage = index_data.get("phan_tram", 0)
            all_indices.append(
                {
                    "name": index_name,
                    "percentage": percentage,
                    "points": index_data.get("diem", 0),
                    "change": index_data.get("thay_doi", 0),
                    "direction": index_data.get("huong", ""),
                    "volume_billion": index_data.get("gtgd_ty_dong", 0),
                    "volume_million_shares": index_data.get("klgd_trieu_cp", 0),
                    "stocks_info": index_data.get("cp_tang_giam", {}),
                }
            )
        sorted_indices = sorted(
            all_indices, key=lambda x: x["percentage"], reverse=True
        )
        top_gaining_indices = [idx for idx in sorted_indices if idx["percentage"] > 0][
            :top_count
        ]
        top_losing_indices = [idx for idx in sorted_indices if idx["percentage"] < 0][
            :top_count
        ]
        if len(top_losing_indices) < top_count:
            remaining_count = top_count - len(top_losing_indices)
            neutral_or_low_gain = [
                idx for idx in sorted_indices if idx["percentage"] >= 0
            ][-remaining_count:]
            top_losing_indices.extend(neutral_or_low_gain)
        market_breadth = _analyze_market_breadth(data)
        overall_performance = _analyze_overall_performance(all_indices)
        result = {
            "success": True,
            "top_gaining_indices": top_gaining_indices,
            "top_losing_indices": top_losing_indices,
            "market_breadth": market_breadth,
            "overall_performance": overall_performance,
            "summary": {
                "total_indices": len(all_indices),
                "positive_indices": len(
                    [idx for idx in all_indices if idx["percentage"] > 0]
                ),
                "negative_indices": len(
                    [idx for idx in all_indices if idx["percentage"] < 0]
                ),
                "neutral_indices": len(
                    [idx for idx in all_indices if idx["percentage"] == 0]
                ),
                "avg_gain": sum(idx["percentage"] for idx in top_gaining_indices)
                / len(top_gaining_indices)
                if top_gaining_indices
                else 0,
                "avg_loss": sum(
                    idx["percentage"]
                    for idx in top_losing_indices
                    if idx["percentage"] < 0
                )
                / len([idx for idx in top_losing_indices if idx["percentage"] < 0])
                if any(idx["percentage"] < 0 for idx in top_losing_indices)
                else 0,
                "highest_percentage": max(all_indices, key=lambda x: x["percentage"])[
                    "percentage"
                ],
                "lowest_percentage": min(all_indices, key=lambda x: x["percentage"])[
                    "percentage"
                ],
            },
        }
        return result
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "top_gaining_indices": [],
            "top_losing_indices": [],
        }


def _analyze_market_breadth(data):
    """Phân tích độ rộng thị trường dựa trên VNINDEX"""
    vnindex = data.get("vnindex_summary") or data.get("market_indices", {}).get(
        "VNINDEX", {}
    )
    cp_tang_giam = vnindex.get("cp_tang_giam", {})
    tang = cp_tang_giam.get("tang", 0)
    giam = cp_tang_giam.get("giam", 0)
    khong_doi = cp_tang_giam.get("khong_doi", 0)
    total = tang + giam + khong_doi
    if total == 0:
        return {"status": "unknown", "description": "Không có dữ liệu"}
    advance_pct = (tang / total) * 100
    if advance_pct > 60:
        status = "very_positive"
        description = "Thị trường rất tích cực"
    elif advance_pct > 50:
        status = "positive"
        description = "Thị trường tích cực"
    elif advance_pct > 40:
        status = "neutral"
        description = "Thị trường trung tính"
    else:
        status = "negative"
        description = "Thị trường tiêu cực"
    return {
        "status": status,
        "description": description,
        "advance_percentage": round(advance_pct, 1),
        "decline_percentage": round((giam / total) * 100, 1),
        "unchanged_percentage": round((khong_doi / total) * 100, 1),
        "total_stocks": total,
    }


def _analyze_overall_performance(all_indices):
    """Phân tích hiệu suất tổng thể của thị trường"""
    total_volume = sum(idx["volume_billion"] for idx in all_indices)
    weighted_avg_percentage = (
        sum(idx["percentage"] * idx["volume_billion"] for idx in all_indices)
        / total_volume
        if total_volume > 0
        else 0
    )
    positive_count = len([idx for idx in all_indices if idx["percentage"] > 0])
    total_count = len(all_indices)
    market_sentiment = "positive" if positive_count > total_count / 2 else "negative"
    return {
        "total_volume_billion": round(total_volume, 2),
        "weighted_avg_percentage": round(weighted_avg_percentage, 3),
        "market_sentiment": market_sentiment,
        "positive_ratio": round(positive_count / total_count * 100, 1),
        "strongest_performer": max(all_indices, key=lambda x: x["percentage"])["name"],
        "weakest_performer": min(all_indices, key=lambda x: x["percentage"])["name"],
    }
