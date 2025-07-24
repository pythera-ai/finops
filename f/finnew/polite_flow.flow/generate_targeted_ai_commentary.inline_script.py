import requests
from typing import Dict, Any


def build_dynamic_prompt(prepared_data: Dict[str, Any]) -> str:
    """Builds a Gemini prompt dynamically based on the selected indices."""
    display_name = prepared_data.get("display_name", "the market")
    analysis_data = prepared_data.get("analysis_data", {})

    prompt_header = f"Bạn là một chuyên gia phân tích tài chính. Hãy đưa ra nhận định về {display_name} dựa trên dữ liệu sau:\n\n"

    data_details = []
    for name, data in analysis_data.items():
        points = data.get("diem", "N/A")
        percent = data.get("phan_tram", 0)
        volume = data.get("gtgd_ty_dong", "N/A")
        change_str = f"+{percent:.2f}%" if percent > 0 else f"{percent:.2f}%"

        data_details.append(
            f"Chỉ số {name}:\n"
            f"- Điểm số: {points:,.2f}\n"
            f"- Thay đổi: {change_str}\n"
            f"- Khối lượng giao dịch: {volume:,.0f} tỷ đồng\n"
        )

    prompt_body = "\n".join(data_details)
    prompt_footer = "\n\nHãy cung cấp 3-4 nhận định chính về xu hướng, tâm lý nhà đầu tư và khuyến nghị ngắn hạn. Viết ngắn gọn, bắt đầu mỗi ý bằng dấu gạch ngang (-)."

    return prompt_header + prompt_body + prompt_footer


def main(
    prepared_data: Dict[str, Any], gemini_api_key: str, analysis_type: str = "standard"
) -> Dict[str, Any]:
    if not prepared_data or not prepared_data.get("success"):
        return {"success": False, "analysis": "Dữ liệu không hợp lệ để phân tích."}

    try:
        prompt = build_dynamic_prompt(prepared_data)
        print(prompt)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={gemini_api_key}"
        headers = {"Content-Type": "application/json"}
        payload = {"contents": [{"parts": [{"text": prompt}]}]}

        response = requests.post(url, headers=headers, json=payload, timeout=45)
        response.raise_for_status()

        result = response.json()
        analysis_text = result["candidates"][0]["content"]["parts"][0]["text"]
        return {"success": True, "analysis": analysis_text.strip()}

    except Exception as e:
        return {"success": False, "analysis": f"Lỗi khi tạo nhận định AI: {e}"}
