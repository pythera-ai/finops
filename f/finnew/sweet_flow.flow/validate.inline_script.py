# import wmill
from typing import Dict, Any, Optional
import logging


def main(raw_market_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Xử lý và validate dữ liệu thị trường

    Args:
        raw_market_data: Dữ liệu thị trường thô
        trading_date: Ngày giao dịch (optional)

    Returns:
        Dữ liệu đã được xử lý và validate
    """

    try:
        # Validate dữ liệu đầu vào
        if not raw_market_data or not isinstance(raw_market_data, dict):
            raise ValueError("Invalid market data format")

        raw_market_data = raw_market_data.get("data", "")
        # Chuẩn hóa cấu trúc dữ liệu
        processed_data = {
            "success": True,
            "data": {
                "trading_date": raw_market_data.get("trading_date"),
                "vnindex_summary": raw_market_data.get("vnindex_summary", {}),
                "foreign_investors": raw_market_data.get("additional_info", {}).get(
                    "foreign_investors", {}
                ),
                "analysis": raw_market_data.get("analysis", {}),
                "additional_info": raw_market_data.get("additional_info", {}),
                "market_indices": raw_market_data.get("market_indices", {}),
            },
        }

        # Validate các trường bắt buộc
        vnindex = processed_data["data"]["vnindex_summary"]
        required_fields = ["diem", "thay_doi", "phan_tram", "gtgd_ty_dong"]

        for field in required_fields:
            if field not in vnindex:
                logging.info(f"Warning: Missing field {field} in vnindex_summary")

        # Tính toán metrics cơ bản
        cp_tang_giam = vnindex.get("cp_tang_giam", {})
        tang = cp_tang_giam.get("tang", 0)
        giam = cp_tang_giam.get("giam", 0)
        khong_doi = cp_tang_giam.get("khong_doi", 0)

        processed_data["data"]["basic_metrics"] = {
            # "total_stocks": tang + giam + khong_doi,
            # "advance_ratio": tang / (tang + giam) if (tang + giam) > 0 else 0,
            "volume_strength": "high"
            if vnindex.get("gtgd_ty_dong", 0) > 15000
            else "medium"
            if vnindex.get("gtgd_ty_dong", 0) > 10000
            else "low",
        }

        logging.info(
            f"Successfully processed market data for {processed_data['data']['trading_date']}"
        )

        return processed_data

    except Exception as e:
        logging.error(f"Error processing market data: {str(e)}")
        return {"success": False, "error": str(e), "data": {}}
