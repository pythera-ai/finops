# import wmill
from typing import Dict, Any, List
from datetime import datetime

# --- Helper Functions for Report Generation ---


def _format_index_line(index_name: str, index_data: Dict[str, Any]) -> str:
    """
    Formats a single, clean summary line for a given market index.

    Args:
        index_name: The name of the index (e.g., "VNINDEX").
        index_data: The dictionary containing data for that index.

    Returns:
        A formatted string line for the report.
    """
    # Fallback to 0 if data is missing
    points = index_data.get("diem", 0)
    change = index_data.get("thay_doi", 0)
    percent = index_data.get("phan_tram", 0)

    # Determine the trend icon based on the percentage change
    if percent > 0:
        icon = "▲"
        # Format with an explicit '+' for positive changes
        formatted_percent = f"+{percent:,.2f}%"
    elif percent < 0:
        icon = "▼"
        formatted_percent = f"{percent:,.2f}%"
    else:
        icon = "➖"
        formatted_percent = f"{percent:,.2f}%"

    # Standardize index names for better display
    display_name = index_name.upper()

    return (
        f"📊 {display_name}: {points:,.2f} điểm "
        f"({icon}{change:,.2f} | {formatted_percent})\n\n"
        f"💰 GTGD Toàn Thị Trường: {index_data.get('gtgd_ty_dong', 0):,.0f} tỷ đồng"
    )


def _format_foreign_investors(foreign_data: Dict[str, Any]) -> str:
    """Formats the foreign investors summary line."""
    if not foreign_data:
        return ""

    direction = foreign_data.get("tien_direction")
    value = foreign_data.get("tien_ty_dong", 0)

    status = "Mua ròng" if direction == "up" else "Bán ròng"

    return f"🌍 Khối ngoại: {status} {value:,.0f} tỷ"


def _format_top_stocks(stock_indices: Dict[str, Any]) -> List[str]:
    """
    Formats the 'Top Gaining' and 'Top Losing' sections of the report.

    Returns:
        A list of formatted strings for the report.
    """
    if not stock_indices or not stock_indices.get("success"):
        return []

    report_parts = []

    # Format top gainers
    top_gainers = stock_indices.get("top_gaining_indices", [])
    if top_gainers:
        gainer_details = " | ".join(
            [f"{idx['name']} (+{idx['percentage']:.2f}%)" for idx in top_gainers]
        )
        report_parts.append(f"🔥 Top Tăng Mạnh:\n✅ {gainer_details}")

    # Format top losers (only include actual negative stocks)
    top_losers = stock_indices.get("top_losing_indices", [])
    actual_losers = [idx for idx in top_losers if idx.get("percentage", 0) < 0]
    if actual_losers:
        loser_details = " | ".join(
            [f"{idx['name']} ({idx['percentage']:.2f}%)" for idx in actual_losers]
        )
        report_parts.append(f"❌ Top Giảm:\n{loser_details}")

    return report_parts


def _format_hot_sectors(data: Dict[str, Any]) -> str:
    """Formats the 'Hot Sectors' section."""
    hot_sectors = data.get("additional_info", {}).get("hot_sectors", [])
    if hot_sectors:
        return f"‼️Ngành nổi bật:\n{', '.join(hot_sectors)}"
    return ""


def _format_ai_analysis(ai_analysis: Dict[str, Any]) -> str:
    """Formats the AI analysis section."""
    if ai_analysis and ai_analysis.get("success"):
        analysis_text = ai_analysis.get("analysis", "").strip()
        if analysis_text:
            return f"🧠 Nhận định AI:\n{analysis_text}"
    return ""


def _get_footer() -> str:
    """Returns the standard report footer."""
    return (
        "📱 Nhận tổng kết mỗi ngày?\n"
        '📩 Nhắn "TTCK" để đăng ký tin tự động.\n\n'
        "🎯 Phân tích sâu – Cập nhật liên tục – Định hướng đầu tư chuẩn xác"
    )


# --- Main Report Generation Functions ---


def _create_telegram_report(
    data: Dict[str, Any], ai_analysis: Dict[str, Any], stock_indices: Dict[str, Any]
) -> str:
    """
    Creates a clean, well-formatted report for Telegram.
    This version includes summaries for VNINDEX, HNXINDEX, and UPCOM.
    """
    # 1. Extract all necessary data at the top
    trading_date = data.get("trading_date", datetime.now().strftime("%d/%m/%Y"))
    all_market_indices = data.get("market_indices", {})
    foreign_data = data.get("additional_info", {}).get("foreign_investors", {})

    # 2. Build the report section by section using a list
    report_parts = []

    # Header
    report_parts.append(f"🔔 [TỔNG KẾT THỊ TRƯỜNG - {trading_date}]")

    # Major Indices Summary (VNINDEX, HNXINDEX, UPCOM)
    indices_to_show = ["VNINDEX", "HNXINDEX", "UPCOM"]
    for index_name in indices_to_show:
        if index_name in all_market_indices:
            report_parts.append(
                _format_index_line(index_name, all_market_indices[index_name])
            )

    report_parts.append(_format_foreign_investors(foreign_data))

    # Top Gainers/Losers
    report_parts.extend(_format_top_stocks(stock_indices))

    # Hot Sectors
    report_parts.append(_format_hot_sectors(data))

    # AI Analysis
    report_parts.append(_format_ai_analysis(ai_analysis))

    # Footer
    report_parts.append(_get_footer())

    # 3. Join all parts with appropriate spacing
    # Filter out any empty strings that might have been added
    return "\n\n".join(filter(None, report_parts))


def _create_email_report(
    data: Dict[str, Any], ai_analysis: Dict[str, Any], stock_indices: Dict[str, Any]
) -> str:
    """Creates a detailed report suitable for an email format."""
    # This function can be similarly refactored using the helper functions
    # For now, it remains as a placeholder for future enhancement
    trading_date = data.get("trading_date", "N/A")
    return f"Subject: Báo cáo thị trường chứng khoán - {trading_date}\n\nEmail report generation is a placeholder."


def _create_web_report(
    data: Dict[str, Any], ai_analysis: Dict[str, Any], stock_indices: Dict[str, Any]
) -> str:
    """Creates a rich Markdown report for web pages."""
    # This function can also be refactored using the same principles
    return "Web report generation is a placeholder."


# --- Main Dispatcher Function ---


def main(
    market_data: Dict[str, Any],
    ai_analysis: Dict[str, Any],
    stock_indices: Dict[str, Any],
    report_style: str = "telegram",
) -> Dict[str, Any]:
    """
    Main entry point to generate a market report in the specified style.

    Args:
        market_data: The processed market data from previous steps.
        ai_analysis: The AI-generated commentary.
        stock_indices: The analysis of top/bottom performing indices.
        report_style: The desired output format ('telegram', 'email', 'web').

    Returns:
        A dictionary containing the generated report and metadata.
    """
    try:
        # Validate input data
        if not market_data or not market_data.get("success"):
            raise ValueError("Invalid or failed market_data input.")

        data = market_data.get("data", {})
        if not data:
            raise ValueError("The 'data' key is missing from market_data.")

        # A mapping of report styles to their respective generator functions
        report_generators = {
            "telegram": _create_telegram_report,
            "email": _create_email_report,
            "web": _create_web_report,
        }

        # Select the appropriate generator, defaulting to Telegram
        generator_func = report_generators.get(report_style, _create_telegram_report)

        # Generate the report
        report_content = generator_func(data, ai_analysis, stock_indices)

        return {
            "success": True,
            "report": report_content,
            "report_style": report_style,
            "word_count": len(report_content.split()),
            "generated_at": datetime.now().isoformat(),
        }

    except Exception as e:
        # Return a structured error message
        return {"success": False, "error": str(e), "report": ""}
