# import wmill
from typing import Dict, Any
from datetime import datetime


def main(
    market_data: Dict[str, Any],
    ai_analysis: Dict[str, Any],
    stock_indices: Dict[str, Any],  # Đổi từ stock_performance sang stock_indices
    report_style: str = "telegram",
) -> Dict[str, Any]:
    try:
        if not market_data.get("success"):
            raise ValueError("Invalid market data")

        data = market_data["data"]

        # Chọn template dựa trên style
        if report_style == "telegram":
            report = _create_telegram_report(data, ai_analysis, stock_indices)
        elif report_style == "email":
            report = _create_email_report(data, ai_analysis, stock_indices)
        elif report_style == "web":
            report = _create_web_report(data, ai_analysis, stock_indices)
        else:
            report = _create_telegram_report(data, ai_analysis, stock_indices)

        print(f"Successfully generated {report_style} report")

        return {
            "success": True,
            "report": report,
            "report_style": report_style,
            "word_count": len(report.split()),
            "generated_at": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"Error generating text report: {str(e)}")
        return {"success": False, "error": str(e), "report": ""}


def _create_telegram_report(data, ai_analysis, stock_indices):
    """Tạo báo cáo cho Telegram"""
    vnindex = data.get("vnindex_summary", {})
    foreign_data = data.get("foreign_investors", {})
    trading_date = data.get("trading_date", datetime.now().strftime("%d/%m/%Y"))

    foreign_status = (
        "Mua ròng" if foreign_data.get("tien_direction") == "up" else "Bán ròng"
    )

    report = f"""🔔 [TỔNG KẾT THỊ TRƯỜNG - {trading_date}]
📊 VN-Index: {vnindex.get("diem", "N/A")} điểm (▲{vnindex.get("thay_doi", "N/A")} | +{vnindex.get("phan_tram", "N/A")}%)
💰 GTGD: {vnindex.get("gtgd_ty_dong", 0):,.0f} tỷ đồng
🌍 Khối ngoại: {foreign_status} {foreign_data.get("tien_ty_dong", 0)} tỷ
"""

    # Thêm thông tin chỉ số thị trường
    if stock_indices.get("success"):
        top_gainers = stock_indices.get("top_gaining_indices", [])
        top_losers = stock_indices.get("top_losing_indices", [])

        if top_gainers:
            report += f"\n\n🔥 Chỉ số tăng mạnh:"
            string_format = " | ".join(
                [f"{idx['name']} (+{idx['percentage']:.2f}%)" for idx in top_gainers]
            )
            report += f"\n✅ " + string_format

        if top_losers:
            # Lọc chỉ những chỉ số thực sự giảm
            actual_losers = [idx for idx in top_losers if idx["percentage"] < 0]
            if actual_losers:
                temp_str = " | ".join(
                    [
                        f"{idx['name']} ({idx['percentage']:.2f}%)"
                        for idx in actual_losers
                    ]
                )
                report += f"\n❌ " + temp_str

    # Thêm ngành hot
    hot_sectors = data.get("additional_info", {}).get("hot_sectors", [])
    if hot_sectors:
        report += f"\n\n📊 Ngành nổi bật:\n{', '.join(hot_sectors)}"

    # Thêm phân tích AI
    if ai_analysis.get("success"):
        report += f"\n\n🧠 Nhận định AI:\n{ai_analysis.get('analysis', '')}"

    # Thêm phần đăng ký nhận tin
    report += f"\n\n📱 Nhận tổng kết mỗi ngày?"
    report += f"\n📩 Nhắn TTCK để đăng ký tin tự động"
    report += f"\n\n🎯 Phân tích sâu – Cập nhật liên tục – Định hướng đầu tư chuẩn xác"

    return report


def _create_email_report(data, ai_analysis, stock_indices):
    """Tạo báo cáo cho Email"""
    vnindex = data.get("vnindex_summary", {})
    trading_date = data.get("trading_date", datetime.now().strftime("%d/%m/%Y"))

    report = f"""Subject: Báo cáo thị trường chứng khoán - {trading_date}

Kính chào Quý khách,

Đây là báo cáo tổng kết thị trường chứng khoán ngày {trading_date}:

I. TỔNG QUAN THỊ TRƯỜNG
• VN-Index: {vnindex.get("diem", "N/A")} điểm ({vnindex.get("phan_tram", "N/A")}%)
• Giá trị giao dịch: {vnindex.get("gtgd_ty_dong", 0):,.0f} tỷ đồng
• Khối ngoại: {data.get("foreign_investors", {}).get("tien_direction", "N/A")} {data.get("foreign_investors", {}).get("tien_ty_dong", 0)} tỷ

II. DIỄN BIẾN CHỈ SỐ THỊ TRƯỜNG"""

    if stock_indices.get("success"):
        breadth = stock_indices.get("market_breadth", {})
        overall_perf = stock_indices.get("overall_performance", {})

        report += f"\n• Độ rộng thị trường: {breadth.get('description', 'N/A')}"
        report += (
            f"\n• Tâm lý thị trường: {overall_perf.get('market_sentiment', 'N/A')}"
        )

        top_gainers = stock_indices.get("top_gaining_indices", [])
        if top_gainers:
            temp_str = ", ".join(
                [f"{idx['name']} (+{idx['percentage']:.2f}%)" for idx in top_gainers]
            )
            report += f"\n• Chỉ số tăng mạnh: {temp_str}"

        top_losers = [
            idx
            for idx in stock_indices.get("top_losing_indices", [])
            if idx["percentage"] < 0
        ]
        if top_losers:
            temp_str = ", ".join(
                [f"{idx['name']} ({idx['percentage']:.2f}%)" for idx in top_losers]
            )
            report += f"\n• Chỉ số giảm: {temp_str}"

    if ai_analysis.get("success"):
        report += f"\n\nIII. NHẬN ĐỊNH CHUYÊN GIA\n{ai_analysis.get('analysis', '')}"

    report += f"\n\n📱 Nhận tổng kết mỗi ngày?"
    report += f"\n📩 Nhắn TTCK để đăng ký tin tự động"
    report += f"\n\nTrân trọng,\nTeam phân tích thị trường"

    return report


def _create_web_report(data, ai_analysis, stock_indices):
    """Tạo báo cáo cho Web (Markdown)"""
    vnindex = data.get("vnindex_summary", {})
    foreign_data = data.get("foreign_investors", {})
    trading_date = data.get("trading_date", datetime.now().strftime("%d/%m/%Y"))

    # Xác định trend icon
    vnindex_trend = (
        "🔺"
        if vnindex.get("phan_tram", 0) > 0
        else "🔻"
        if vnindex.get("phan_tram", 0) < 0
        else "➖"
    )
    foreign_trend = "💰" if foreign_data.get("tien_direction") == "up" else "💸"

    # Header với styling đẹp
    report = f"""# 📈 Báo cáo thị trường chứng khoán
## 📅 Ngày {trading_date}

---

## 📊 Tổng quan thị trường

> **Tình hình giao dịch và biến động chỉ số chính**

| 📋 **Chỉ số** | 💹 **Giá trị** | 📈 **Thay đổi** | 🎯 **Trạng thái** |
|-------------|-------------|-------------|-------------|
| **VN-Index** | `{vnindex.get("diem", "N/A")}` điểm | `{vnindex.get("thay_doi", "N/A")}` ({vnindex.get("phan_tram", "N/A")}%) | {vnindex_trend} |
| **GTGD** | `{vnindex.get("gtgd_ty_dong", 0):,.0f}` tỷ VND | `{vnindex.get("klgd_trieu_cp", 0):,.0f}` triệu CP | 📊 |
| **Khối ngoại** | `{foreign_data.get("tien_ty_dong", 0):,.0f}` tỷ VND | `{foreign_data.get("cp_trieu", 0):,.0f}` triệu CP | {foreign_trend} |

### 🎯 Thống kê nhanh
- **Cổ phiếu tăng**: {vnindex.get("cp_tang_giam", {}).get("tang", 0)} mã
- **Cổ phiếu giảm**: {vnindex.get("cp_tang_giam", {}).get("giam", 0)} mã  
- **Cổ phiếu đứng giá**: {vnindex.get("cp_tang_giam", {}).get("khong_doi", 0)} mã

---

## 🔥 Diễn biến chỉ số thị trường"""

    if stock_indices.get("success"):
        top_gainers = stock_indices.get("top_gaining_indices", [])
        top_losers = [
            idx
            for idx in stock_indices.get("top_losing_indices", [])
            if idx["percentage"] < 0
        ]
        market_breadth = stock_indices.get("market_breadth", {})
        overall_perf = stock_indices.get("overall_performance", {})

        # Market breadth analysis
        if market_breadth:
            breadth_emoji = (
                "🟢"
                if market_breadth.get("status") == "positive"
                else "🔴"
                if market_breadth.get("status") == "negative"
                else "🟡"
            )
            report += f"""

### {breadth_emoji} Độ rộng thị trường
> **{market_breadth.get("description", "N/A")}**

| Trạng thái | Tỷ lệ | Số lượng |
|-----------|-------|----------|
| 🟢 Tăng | `{market_breadth.get("advance_percentage", 0):.1f}%` | {vnindex.get("cp_tang_giam", {}).get("tang", 0)} mã |
| 🔴 Giảm | `{market_breadth.get("decline_percentage", 0):.1f}%` | {vnindex.get("cp_tang_giam", {}).get("giam", 0)} mã |
| ⚪ Đứng giá | `{market_breadth.get("unchanged_percentage", 0):.1f}%` | {vnindex.get("cp_tang_giam", {}).get("khong_doi", 0)} mã |
"""

        # Top gainers
        if top_gainers:
            report += f"""
### ✅ Chỉ số tăng mạnh nhất

| 🏆 **Chỉ số** | 💹 **Điểm số** | 📈 **Thay đổi** | 💰 **Khối lượng** |
|-------------|-------------|-------------|-------------|"""

            for idx in top_gainers:
                volume_text = (
                    f"{idx.get('volume_billion', 0):,.0f} tỷ"
                    if idx.get("volume_billion")
                    else "N/A"
                )
                report += f"""
| **{idx["name"]}** | `{idx["points"]:.2f}` | `+{idx["percentage"]:.2f}%` | {volume_text} |"""

        # Top losers
        if top_losers:
            report += f"""

### ❌ Chỉ số giảm mạnh nhất

| 📉 **Chỉ số** | 💹 **Điểm số** | 📈 **Thay đổi** | 💰 **Khối lượng** |
|-------------|-------------|-------------|-------------|"""

            for idx in top_losers:
                volume_text = (
                    f"{idx.get('volume_billion', 0):,.0f} tỷ"
                    if idx.get("volume_billion")
                    else "N/A"
                )
                report += f"""
| **{idx["name"]}** | `{idx["points"]:.2f}` | `{idx["percentage"]:.2f}%` | {volume_text} |"""

    # Hot sectors
    hot_sectors = data.get("additional_info", {}).get("hot_sectors", [])
    if hot_sectors:
        report += f"""

---

## 🔥 Ngành nổi bật

> **Các ngành được quan tâm nhất trong phiên**

"""
        for i, sector in enumerate(hot_sectors, 1):
            report += f"{i}. **{sector}** 🎯\n"

    # Focus stocks
    focus_stocks = data.get("additional_info", {}).get("focus_stocks", [])
    if focus_stocks:
        report += f"""

### 👀 Cổ phiếu được chú ý

`{" | ".join(focus_stocks[:10])}`
"""

    # AI Analysis
    if ai_analysis.get("success") and ai_analysis.get("analysis"):
        report += f"""

---

## 🧠 Nhận định chuyên gia

> **Phân tích chuyên sâu từ AI về xu hướng thị trường**

{ai_analysis.get("analysis", "")}
"""

    # Subscription section
    report += f"""

---

## 📱 Đăng ký nhận tin

> **Nhận tổng kết mỗi ngày?**

📩 **Nhắn TTCK** để đăng ký tin tự động

🎯 *Phân tích sâu • Cập nhật liên tục • Định hướng đầu tư chuẩn xác*
"""

    # Footer
    report += f"""

---

## 📋 Thông tin báo cáo

| **Thông tin** | **Chi tiết** |
|-------------|-------------|
| **Ngày giao dịch** | {trading_date} |
| **Thời gian tạo** | {datetime.now().strftime("%H:%M:%S %d/%m/%Y")} |
| **Phiên bản** | Auto-generated Report v2.0 |

---

<div align="center">
  <strong>💡 Báo cáo được tạo tự động bởi hệ thống phân tích thị trường</strong><br>
  <em>🔄 Cập nhật liên tục • 📊 Dữ liệu chính xác • 🎯 Phân tích chuyên sâu</em>
</div>
"""

    return report
