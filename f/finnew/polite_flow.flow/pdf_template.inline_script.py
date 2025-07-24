from typing import Dict, Any
import json
from datetime import datetime
import boto3
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from io import BytesIO
import tempfile
import os

import gdown


def download_freemono_font():
    """Tải font FreeMono nếu không có sẵn"""
    try:
        import urllib.request
        import tempfile

        # URL font FreeMono từ GNU FreeFont
        font_url = "https://github.com/opensourcedesign/fonts/raw/master/gnu-freefont_freemono/FreeMono.ttf"

        # Tạo temp file cho font
        with tempfile.NamedTemporaryFile(delete=False, suffix=".ttf") as temp_font:
            urllib.request.urlretrieve(font_url, temp_font.name)
            print(f"Downloaded FreeMono font to: {temp_font.name}")
            return temp_font.name

    except Exception as e:
        print(f"Failed to download FreeMono font: {e}")
        return None


def register_freemono_font():
    """Đăng ký font FreeMono"""
    try:
        # Thử tìm font FreeMono trong hệ thống
        font_paths = [
            "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",  # Ubuntu/Debian
            "/usr/share/fonts/truetype/liberation/LiberationMono-Bold.ttf",  # Ubuntu/Debian Bold
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",  # Alternative
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",  # Alternative Bold
            "/System/Library/Fonts/Menlo.ttc",  # macOS
            "C:\\Windows\\Fonts\\consola.ttf",  # Windows Console
        ]

        font_registered = False
        for font_path in font_paths:
            if os.path.exists(font_path):
                try:
                    if "Bold" in font_path or "bold" in font_path:
                        pdfmetrics.registerFont(TTFont("FreeMono-Bold", font_path))
                    else:
                        pdfmetrics.registerFont(TTFont("FreeMono", font_path))
                    print(f"Successfully registered font: {font_path}")
                    font_registered = True
                except Exception as e:
                    print(f"Failed to register font {font_path}: {e}")
                    continue

        # Nếu không tìm thấy font, thử tải về
        if not font_registered:
            downloaded_font = download_freemono_font()
            if downloaded_font:
                try:
                    pdfmetrics.registerFont(TTFont("FreeMono", downloaded_font))
                    pdfmetrics.registerFont(
                        TTFont("FreeMono-Bold", downloaded_font)
                    )  # Use same for bold
                    font_registered = True
                    print("Successfully registered downloaded FreeMono font")
                except Exception as e:
                    print(f"Failed to register downloaded font: {e}")

        if font_registered:
            return "FreeMono", "FreeMono-Bold"
        else:
            print("FreeMono font not available, using Helvetica as fallback")
            return "Helvetica", "Helvetica-Bold"

    except Exception as e:
        print(f"Font registration error: {e}, using Helvetica")
        return "Helvetica", "Helvetica-Bold"


def main(
    market_data: Dict[str, Any],
    ai_analysis: Dict[str, Any],
    stock_indices: Dict[str, Any],  # Đổi từ stock_performance sang stock_indices
    report_title: str = "Báo cáo thị trường chứng khoán",
    s3_bucket: str = "your-bucket-name",
    aws_access_key_id: str = "",
    aws_secret_access_key: str = "",
    aws_region: str = "us-east-1",
) -> Dict[str, Any]:
    """
    Tạo báo cáo PDF và upload lên S3

    Args:
        market_data: Dữ liệu thị trường
        ai_analysis: Phân tích AI
        stock_indices: Thông tin chỉ số thị trường (đổi từ stock_performance)
        report_title: Tiêu đề báo cáo
        s3_bucket: Tên S3 bucket
        aws_access_key_id: AWS Access Key ID
        aws_secret_access_key: AWS Secret Access Key
        aws_region: AWS Region

    Returns:
        Kết quả upload và thông tin file
    """

    try:
        # Chuẩn bị dữ liệu PDF
        pdf_data = prepare_pdf_data(
            market_data, ai_analysis, stock_indices, report_title
        )

        if not pdf_data["success"]:
            return pdf_data

        # Tạo PDF
        pdf_buffer = create_pdf_report(pdf_data["pdf_data"])

        # Upload lên S3
        s3_result = upload_to_s3(
            pdf_buffer,
            pdf_data["file_name"],
            s3_bucket,
            aws_access_key_id,
            aws_secret_access_key,
            aws_region,
        )

        if s3_result["success"]:
            result = {
                "success": True,
                "pdf_generated": True,
                "s3_uploaded": True,
                "file_name": pdf_data["file_name"],
                "s3_url": s3_result["s3_url"],
                "file_size": len(pdf_buffer.getvalue()),
                "summary_stats": pdf_data["summary_stats"],
            }

            print(f"Successfully created and uploaded PDF: {pdf_data['file_name']}")
            return result
        else:
            return s3_result

    except Exception as e:
        raise e
        return {"success": False, "error": str(e)}


def prepare_pdf_data(market_data, ai_analysis, stock_indices, report_title):
    """Chuẩn bị dữ liệu cho PDF (đã cập nhật để sử dụng stock_indices)"""
    try:
        if not market_data.get("success"):
            raise ValueError("Invalid market data")

        data = market_data["data"]
        vnindex = data.get("vnindex_summary", {})
        foreign_data = data.get("foreign_investors", {})
        trading_date = data.get("trading_date", datetime.now().strftime("%d/%m/%Y"))

        # Chuẩn bị dữ liệu cho PDF
        pdf_data = {
            "title": report_title,
            "date": trading_date,
            "generated_at": datetime.now().isoformat(),
            "market_overview": {
                "vnindex_points": vnindex.get("diem", "N/A"),
                "change_points": vnindex.get("thay_doi", "N/A"),
                "change_percent": vnindex.get("phan_tram", "N/A"),
                "volume": vnindex.get("gtgd_ty_dong", 0),
                "foreign_net": foreign_data.get("tien_ty_dong", 0),
                "foreign_direction": foreign_data.get("tien_direction", "N/A"),
            },
            "stock_indices": stock_indices  # Đổi từ stock_performance sang stock_indices
            if stock_indices.get("success")
            else {},
            "ai_analysis": ai_analysis.get("analysis", "").replace("\n-", "")
            if ai_analysis.get("success")
            else "",
            "market_breadth": stock_indices.get("market_breadth", {})
            if stock_indices.get("success")
            else {},
            "hot_sectors": data.get("additional_info", {}).get("hot_sectors", []),
        }

        # Tạo summary cho PDF
        summary_stats = {
            "total_analysis_points": len(ai_analysis.get("analysis", "").split("-"))
            if ai_analysis.get("success")
            else 0,
            "top_gainers_count": len(stock_indices.get("top_gaining_indices", []))
            if stock_indices.get("success")
            else 0,
            "top_losers_count": len(
                [
                    idx
                    for idx in stock_indices.get("top_losing_indices", [])
                    if idx.get("percentage", 0) < 0
                ]
            )
            if stock_indices.get("success")
            else 0,
            "market_sentiment": _determine_market_sentiment(data, stock_indices),
        }

        result = {
            "success": True,
            "pdf_data": pdf_data,
            "summary_stats": summary_stats,
            "file_name": f"market_report_{trading_date.replace('/', '_')}.pdf",
        }
        print(result)
        return result

    except Exception as e:
        print(f"Error preparing PDF data: {str(e)}")
        return {"success": False, "error": str(e), "pdf_data": {}}


def create_pdf_report(pdf_data):
    """Tạo file PDF từ dữ liệu (đã cập nhật để sử dụng stock_indices)"""
    buffer = BytesIO()

    # Đăng ký font FreeMono
    font_name, font_bold = register_freemono_font()

    # Tạo document với A4 size
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=18,
    )

    # Lấy styles
    styles = getSampleStyleSheet()

    # Tạo custom styles với FreeMono
    title_style = ParagraphStyle(
        "CustomTitle",
        parent=styles["Heading1"],
        fontSize=18,
        spaceAfter=30,
        alignment=TA_CENTER,
        textColor=colors.darkblue,
        fontName=font_bold,
    )

    heading_style = ParagraphStyle(
        "CustomHeading",
        parent=styles["Heading2"],
        fontSize=14,
        spaceAfter=12,
        textColor=colors.darkblue,
        fontName=font_bold,
    )

    normal_style = ParagraphStyle(
        "CustomNormal", parent=styles["Normal"], fontName=font_name, fontSize=10
    )

    # Tạo nội dung PDF
    story = []

    # Tiêu đề
    story.append(Paragraph(pdf_data["title"], title_style))
    story.append(Paragraph(f"Ngày: {pdf_data['date']}", normal_style))
    story.append(Spacer(1, 20))

    # Tổng quan thị trường
    story.append(Paragraph("TỔNG QUAN THỊ TRƯỜNG", heading_style))

    market_overview = pdf_data["market_overview"]
    market_data = [
        ["Chỉ số VN-Index", str(market_overview.get("vnindex_points", "N/A"))],
        ["Thay đổi điểm", str(market_overview.get("change_points", "N/A"))],
        ["Thay đổi %", str(market_overview.get("change_percent", "N/A")) + "%"],
        ["Khối lượng giao dịch", f"{market_overview.get('volume', 0):,.0f} tỷ VND"],
        ["Khối ngoại", f"{market_overview.get('foreign_net', 0):,.0f} tỷ VND"],
        ["Xu hướng KN", str(market_overview.get("foreign_direction", "N/A"))],
    ]

    market_table = Table(market_data, colWidths=[3 * inch, 2 * inch])
    market_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (-1, 0), font_bold),
                ("FONTSIZE", (0, 0), (-1, 0), 12),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("FONTNAME", (0, 1), (-1, -1), font_name),
                ("FONTSIZE", (0, 1), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ]
        )
    )

    story.append(market_table)
    story.append(Spacer(1, 20))

    # Phân tích AI
    if pdf_data.get("ai_analysis"):
        story.append(Paragraph("PHÂN TÍCH AI", heading_style))
        story.append(Paragraph(pdf_data["ai_analysis"], normal_style))
        story.append(Spacer(1, 20))

    # Top chỉ số tăng/giảm
    stock_indices = pdf_data.get("stock_indices", {})
    if stock_indices.get("top_gaining_indices"):
        story.append(Paragraph("TOP CHỈ SỐ TĂNG MẠNH", heading_style))

        gaining_data = [["Chỉ số", "Điểm số", "Thay đổi %"]]
        for idx in stock_indices["top_gaining_indices"][:5]:  # Top 5
            gaining_data.append(
                [
                    idx.get("name", "N/A"),
                    f"{idx.get('points', 0):,.2f}",
                    f"{idx.get('percentage', 0):+.2f}%",
                ]
            )

        gaining_table = Table(
            gaining_data, colWidths=[1.5 * inch, 1.5 * inch, 1.5 * inch]
        )
        gaining_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.green),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), font_bold),
                    ("FONTSIZE", (0, 0), (-1, 0), 10),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.lightgreen),
                    ("FONTNAME", (0, 1), (-1, -1), font_name),
                    ("FONTSIZE", (0, 1), (-1, -1), 9),
                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                ]
            )
        )

        story.append(gaining_table)
        story.append(Spacer(1, 15))

    # Top chỉ số giảm
    if stock_indices.get("top_losing_indices"):
        losing_indices = [
            idx
            for idx in stock_indices["top_losing_indices"]
            if idx.get("percentage", 0) < 0
        ]

        if losing_indices:
            story.append(Paragraph("TOP CHỈ SỐ GIẢM", heading_style))

            losing_data = [["Chỉ số", "Điểm số", "Thay đổi %"]]
            for idx in losing_indices[:5]:  # Top 5
                losing_data.append(
                    [
                        idx.get("name", "N/A"),
                        f"{idx.get('points', 0):,.2f}",
                        f"{idx.get('percentage', 0):.2f}%",
                    ]
                )

            losing_table = Table(
                losing_data, colWidths=[1.5 * inch, 1.5 * inch, 1.5 * inch]
            )
            losing_table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.red),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                        ("FONTNAME", (0, 0), (-1, 0), font_bold),
                        ("FONTSIZE", (0, 0), (-1, 0), 10),
                        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                        ("BACKGROUND", (0, 1), (-1, -1), colors.mistyrose),
                        ("FONTNAME", (0, 1), (-1, -1), font_name),
                        ("FONTSIZE", (0, 1), (-1, -1), 9),
                        ("GRID", (0, 0), (-1, -1), 1, colors.black),
                    ]
                )
            )

            story.append(losing_table)
            story.append(Spacer(1, 15))

    # Sectors nóng
    if pdf_data.get("hot_sectors"):
        story.append(Paragraph("NGÀNH NÓNG", heading_style))
        sectors_text = ", ".join(pdf_data["hot_sectors"])
        story.append(Paragraph(sectors_text, normal_style))
        story.append(Spacer(1, 15))

    # Footer
    story.append(Spacer(1, 30))
    story.append(
        Paragraph(
            "Phân tích sâu – Cập nhật liên tục – Định hướng đầu tư chuẩn xác",
            normal_style,
        )
    )

    # Build PDF
    doc.build(story)
    buffer.seek(0)

    return buffer


def upload_to_s3(pdf_buffer, file_name, bucket_name, access_key, secret_key, region):
    """Upload PDF lên S3"""
    temp_file_path = None
    try:
        # Tạo S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

        # Tạo temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_obj:
            temp_file_path = temp_obj.name
            temp_obj.write(pdf_buffer.getvalue())

        print(f"Created temporary file: {temp_file_path}")

        # Upload file using upload_file (local file path)
        s3_client.upload_file(
            temp_file_path,
            bucket_name,
            file_name,
            ExtraArgs={"ContentType": "application/pdf"},
        )

        # Tạo S3 URL
        s3_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{file_name}"

        print(f"Successfully uploaded to S3: {s3_url}")

        return {
            "success": True,
            "s3_url": s3_url,
            "bucket": bucket_name,
            "key": file_name,
            "local_file_path": temp_file_path,
        }

    except Exception as e:
        # print(f"Error uploading to S3: {str(e)}")
        return {"success": False, "error": f"S3 upload failed: {str(e)}"}


def _determine_market_sentiment(data, stock_indices):
    """Xác định tâm lý thị trường dựa trên chỉ số"""
    vnindex = data.get("vnindex_summary", {})
    change_pct = float(vnindex.get("phan_tram", 0))

    if stock_indices.get("success"):
        # Sử dụng overall_performance từ stock_indices
        overall_perf = stock_indices.get("overall_performance", {})
        market_sentiment = overall_perf.get("market_sentiment", "neutral")

        # Kết hợp với breadth analysis
        breadth = stock_indices.get("market_breadth", {})
        advance_pct = breadth.get("advance_percentage", 50)

        if change_pct > 1 and advance_pct > 60 and market_sentiment == "positive":
            return "very_bullish"
        elif change_pct > 0 and advance_pct > 50 and market_sentiment == "positive":
            return "bullish"
        elif change_pct < -1 and advance_pct < 40 and market_sentiment == "negative":
            return "very_bearish"
        elif change_pct < 0 and market_sentiment == "negative":
            return "bearish"
        else:
            return "neutral"
    else:
        if change_pct > 1:
            return "bullish"
        elif change_pct < -1:
            return "bearish"
        else:
            return "neutral"
