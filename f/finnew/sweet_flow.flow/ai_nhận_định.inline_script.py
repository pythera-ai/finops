import wmill
import requests
from typing import Dict, Any
import json


def main(
    market_data: Dict[str, Any], gemini_api_key: str, analysis_type: str = "standard"
) -> Dict[str, Any]:
    """
    Phân tích thị trường bằng Gemini AI

    Args:
        market_data: Dữ liệu thị trường đã xử lý
        gemini_api_key: API key của Gemini
        analysis_type: Loại phân tích (standard, detailed, brief)

    Returns:
        Kết quả phân tích từ AI
    """

    try:
        if not market_data.get("success"):
            raise ValueError("Invalid market data")

        data = market_data["data"]
        vnindex = data.get("vnindex_summary", {})
        foreign_data = data.get("foreign_investors", {})
        basic_metrics = data.get("basic_metrics", {})

        # Tạo prompt dựa trên loại phân tích
        prompts = {
            "standard": _create_standard_prompt(vnindex, foreign_data, basic_metrics),
            "detailed": _create_detailed_prompt(
                vnindex, foreign_data, basic_metrics, data
            ),
            "brief": _create_brief_prompt(vnindex, basic_metrics),
        }

        prompt = prompts.get(analysis_type, prompts["standard"])

        # Gọi Gemini API
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={gemini_api_key}"

        headers = {"Content-Type": "application/json"}

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.7,
                "topK": 40,
                "topP": 0.95,
                "maxOutputTokens": 1000 if analysis_type == "detailed" else 600,
            },
        }

        response = requests.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code == 200:
            result = response.json()
            analysis_text = result["candidates"][0]["content"]["parts"][0]["text"]

            # Format analysis
            formatted_analysis = _format_analysis_output(analysis_text)

            print(f"Successfully generated {analysis_type} analysis")

            return {
                "success": True,
                "analysis": formatted_analysis,
                "analysis_type": analysis_type,
                "token_count": len(analysis_text.split()),
            }

        else:
            print(f"Gemini API error: {response.status_code}")
            return _get_fallback_analysis(vnindex, basic_metrics)

    except Exception as e:
        print(f"Error in Gemini analysis: {str(e)}")
        return _get_fallback_analysis(vnindex, basic_metrics)


def _create_standard_prompt(vnindex, foreign_data, basic_metrics):
    """Tạo prompt chuẩn"""
    return f"""
    Bạn là chuyên gia phân tích thị trường chứng khoán Việt Nam. Phân tích dữ liệu sau:
    
    📊 VN-Index: {vnindex.get("diem", "N/A")} điểm ({vnindex.get("phan_tram", "N/A")}%)
    💰 GTGD: {vnindex.get("gtgd_ty_dong", "N/A")} tỷ đồng
    🏛️ Khối ngoại: {foreign_data.get("tien_direction", "N/A")} {foreign_data.get("tien_ty_dong", 0)} tỷ
    📈 Tỷ lệ mã tăng: {basic_metrics.get("advance_ratio", 0) * 100:.1f}%
    
    Hãy đưa ra 3-4 nhận định chính về:
    1. Xu hướng thị trường
    2. Tâm lý nhà đầu tư
    3. Khuyến nghị ngắn hạn
    
    Viết nhận định ngắn gọn, mỗi ý khoảng 10 từ, kèm số liệu, bắt đầu bằng dấu -, không cần header
    """


def _create_detailed_prompt(vnindex, foreign_data, basic_metrics, data):
    """Tạo prompt chi tiết"""
    hot_sectors = data.get("additional_info", {}).get("hot_sectors", [])

    return f"""
    Phân tích chi tiết thị trường chứng khoán Việt Nam:
    
    📊 THÔNG TIN CHI TIẾT:
    - VN-Index: {vnindex.get("diem", "N/A")} điểm ({vnindex.get("phan_tram", "N/A")}%)
    - GTGD: {vnindex.get("gtgd_ty_dong", "N/A")} tỷ đồng
    - Tỷ lệ mã tăng: {basic_metrics.get("advance_ratio", 0) * 100:.1f}%
    - Khối ngoại: {foreign_data.get("tien_direction", "N/A")} {foreign_data.get("tien_ty_dong", 0)} tỷ
    - Ngành nổi bật: {", ".join(hot_sectors) if hot_sectors else "N/A"}
    
    Hãy phân tích:
    1. Đánh giá tổng quan xu hướng
    2. Phân tích thanh khoản và tâm lý
    3. Tác động của khối ngoại
    4. Phân tích ngành
    5. Dự báo ngắn hạn và khuyến nghị
    
    Mỗi mục 2-3 câu, bắt đầu bằng -
    """


def _create_brief_prompt(vnindex, basic_metrics):
    """Tạo prompt ngắn gọn"""
    return f"""
    Tóm tắt nhanh thị trường hôm nay:
    
    VN-Index: {vnindex.get("diem", "N/A")} điểm ({vnindex.get("phan_tram", "N/A")}%)
    GTGD: {vnindex.get("gtgd_ty_dong", "N/A")} tỷ đồng
    
    Đưa ra 2 nhận định chính và 1 khuyến nghị. Mỗi ý 1 câu ngắn, bắt đầu bằng -
    """


def _format_analysis_output(analysis_text):
    """Format output analysis"""
    lines = analysis_text.strip().split("\n")
    formatted_lines = []

    for line in lines:
        line = line.strip()
        if line and not line.startswith("-"):
            formatted_lines.append(f"- {line}")
        elif line.startswith("-"):
            formatted_lines.append(line)

    return "\n".join(formatted_lines)


def _get_fallback_analysis(vnindex, basic_metrics):
    """Phân tích dự phòng"""
    change_pct = float(vnindex.get("phan_tram", 0))

    if change_pct > 1:
        trend = "tích cực với momentum mạnh"
    elif change_pct > 0:
        trend = "nhẹ tích cực"
    else:
        trend = "tiêu cực với áp lực bán"

    return {
        "success": True,
        "analysis": f"""- Thị trường có xu hướng {trend}
- Thanh khoản ở mức {basic_metrics.get("volume_strength", "trung bình")}
- Nhà đầu tư nên thận trọng và theo dõi diễn biến""",
        "analysis_type": "fallback",
        "token_count": 0,
    }
