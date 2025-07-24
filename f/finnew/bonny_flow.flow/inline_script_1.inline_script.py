import boto3
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, Any


def send_email_gmail_smtp(
    gmail_user: str,
    gmail_password: str,
    to_emails: list,
    subject: str,
    html_body: str,
    text_body: str = None,
):
    """Send email using Gmail SMTP"""

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = gmail_user
        msg["To"] = ", ".join(to_emails)
        msg["Subject"] = subject

        if text_body:
            msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        smtp_server = "smtp.gmail.com"
        port = 587

        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls(context=context)
            server.login(gmail_user, gmail_password)
            server.send_message(msg)

        return {
            "success": True,
            "message": "Email sent successfully via Gmail SMTP",
            "provider": "Gmail",
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"Gmail SMTP error: {str(e)}",
            "provider": "Gmail",
        }


def generate_excel_download_link(excel_result, s3_config):
    """Generate S3 presigned URL for Excel file download only"""

    excel_url = ""

    try:
        s3_client = boto3.client(
            "s3",
            region_name=s3_config.get("region", "ap-southeast-2"),
            aws_access_key_id=s3_config.get("aws_access_key_id"),
            aws_secret_access_key=s3_config.get("aws_secret_access_key"),
        )

        if excel_result.get("success") and excel_result.get("s3_location"):
            bucket, key = excel_result["s3_location"].replace("s3://", "").split("/", 1)
            excel_url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=86400,  # 24 hours
            )

    except Exception as e:
        print(f"Error generating Excel download link: {e}")

    return excel_url


def create_email_content_excel_only(merge_data, excel_url):
    """Create HTML and text email content for Excel file only"""

    report_date = datetime.now().strftime("%d/%m/%Y")
    execution_meta = merge_data.get("execution_metadata", {})
    quality_meta = merge_data.get("data_quality", {})

    # Create HTML email body
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Times New Roman', serif; line-height: 1.6; color: #333; }}
            .header {{ background: linear-gradient(135deg, #4CAF50 0%, #26A69A 100%); color: white; padding: 20px; text-align: center; }}
            .content {{ padding: 20px; }}
            .summary-box {{ background: #f8f9fa; border-left: 4px solid #4CAF50; padding: 15px; margin: 15px 0; }}
            .download-section {{ background: #e8f5e8; padding: 20px; border-radius: 10px; margin: 20px 0; text-align: center; }}
            .download-button {{ 
                display: inline-block; 
                margin: 10px; 
                padding: 15px 30px; 
                background: #4CAF50; 
                color: white; 
                text-decoration: none; 
                border-radius: 8px; 
                font-weight: bold;
                font-size: 16px;
                transition: background-color 0.3s;
            }}
            .download-button:hover {{ background: #45a049; }}
            .excel-icon {{ font-size: 24px; margin-right: 10px; }}
            .success {{ color: #28a745; }}
            .warning {{ color: #ffc107; }}
            .error {{ color: #dc3545; }}
            .footer {{ background: #f8f9fa; padding: 15px; text-align: center; font-size: 12px; color: #666; }}
            .highlight {{ background: #fff3cd; padding: 10px; border-radius: 5px; margin: 10px 0; }}
            .data-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 15px 0; }}
            .data-item {{ background: #f8f9fa; padding: 10px; border-radius: 5px; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 BÁO CÁO THỊ TRƯỜNG CHỨNG KHOÁN</h1>
            <h2>{report_date}</h2>
        </div>
        
        <div class="content">
            <div class="summary-box">
                <h3>⏰ Thời gian tạo báo cáo: {datetime.now().strftime("%H:%M:%S %d/%m/%Y")}</h3>
                <p><strong>Trạng thái:</strong> <span class="success">✅ Báo cáo đã được tạo thành công</span></p>
            </div>
            
            <div class="download-section">
                <h3>📁 TẢI XUỐNG BÁO CÁO EXCEL</h3>
                <p>Báo cáo Excel chi tiết với đầy đủ thông tin thị trường và phân tích</p>
                {'<a href="' + excel_url + '" target="_blank" class="download-button"><span class="excel-icon">📊</span>TẢI XUỐNG BÁO CÁO EXCEL</a>' if excel_url else '<div class="error">❌ Báo cáo Excel không khả dụng</div>'}
                <p style="font-size: 12px; color: #666; margin-top: 10px;">
                    * Liên kết tải xuống có hiệu lực trong 24 giờ
                </p>
            </div>"""

    html_body += """
                </div>"""

    # Add top sectors if available
    top_sectors = merge_data.get("top_sectors", [])
    if top_sectors:
        html_body += f"""
                <div class="highlight">
                    <strong>🏭 Top ngành nổi bật:</strong> {", ".join(top_sectors[:5])}
                </div>"""

    # Add top stocks if available
    top_interested = merge_data.get("top_interested", [])
    if top_interested:
        html_body += f"""
                <div class="highlight">
                    <strong>⭐ Cổ phiếu tâm điểm:</strong> {", ".join(top_interested[:10])}
                </div>"""

    html_body += """
            </div>
            
            <div class="summary-box">
                <h3>📋 HƯỚNG DẪN SỬ DỤNG</h3>
                <ul>
                    <li>📊 <strong>Báo cáo Excel</strong> chứa đầy đủ thông tin chi tiết về các chỉ số thị trường</li>
                    <li>🎨 <strong>Định dạng màu sắc</strong>: Xanh lá = tăng, Đỏ = giảm, Vàng = không đổi</li>
                    <li>📈 <strong>Các cột phần trăm</strong> hiển thị % thay đổi KLGD và GTGD so với phiên trước</li>
                    <li>💾 <strong>Lưu ý</strong>: Tải xuống và lưu file để sử dụng lâu dài</li>
                </ul>
            </div>
        </div>
        
        <div class="footer">
            <p>🤖 Báo cáo được tạo tự động bởi hệ thống Windmill</p>
            <p>⚡ Được gửi qua Gmail SMTP</p>
        </div>
    </body>
    </html>
    """

    # Create plain text version
    text_body = f"""
📊 BÁO CÁO THỊ TRƯỜNG CHỨNG KHOÁN - {report_date}

⏰ THỜI GIAN TẠO: {datetime.now().strftime("%H:%M:%S %d/%m/%Y")}

📁 TẢI XUỐNG:
{"📊 Excel: " + excel_url if excel_url else "📊 Excel: Không khả dụng"}

    # 📈 DỮ LIỆU NỔI BẬT:"""

    #     if khoi_ngoai:
    #         vol = khoi_ngoai.get("vol", 0)
    #         net_value = khoi_ngoai.get("net_value", 0)
    #         text_body += f"""
    # 📈 Khối ngoại: {vol:,.1f} triệu cp, {net_value:,.2f} tỷ đồng"""

    #     if ktd_value:
    #         text_body += f"""
    # 🏢 Khối tự doanh: {ktd_value:,.2f} tỷ đồng"""

    #     if top_sectors:
    #         text_body += f"""
    # 🏭 Ngành nổi bật: {", ".join(top_sectors[:3])}"""

    #     if top_interested:
    #         text_body += f"""
    # ⭐ Cổ phiếu tâm điểm: {", ".join(top_interested[:5])}"""

    text_body += f"""

📋 HƯỚNG DẪN:
- Báo cáo Excel chứa thông tin chi tiết về thị trường
- Liên kết tải xuống có hiệu lực trong 24 giờ
- Định dạng màu sắc: Xanh = tăng, Đỏ = giảm

---
🤖 Email tự động từ hệ thống báo cáo - {datetime.now().strftime("%H:%M:%S %d/%m/%Y")}
⚡ Gửi qua Gmail SMTP
    """

    return html_body, text_body


def main(
    excel_result: Dict[str, Any],
    merge_data: Dict[str, Any],
    to_emails: str,
    cc_emails: str = "",
    bcc_emails: str = "",
    gmail_user: str = "",
    gmail_password: str = "",
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    s3_region: str = "ap-southeast-2",
) -> Dict[str, Any]:
    """Send Gmail notification with Excel file only"""

    # Validate Gmail credentials
    if not gmail_user or not gmail_password:
        return {
            "success": False,
            "message": "Gmail credentials (gmail_user and gmail_password) are required",
            "timestamp": datetime.now().isoformat(),
        }

    # Parse email lists
    to_list = [email.strip() for email in to_emails.split(",") if email.strip()]
    cc_list = (
        [email.strip() for email in cc_emails.split(",") if email.strip()]
        if cc_emails
        else []
    )
    bcc_list = (
        [email.strip() for email in bcc_emails.split(",") if email.strip()]
        if bcc_emails
        else []
    )

    if not to_list:
        return {
            "success": False,
            "message": "No recipient emails provided in to_emails",
            "timestamp": datetime.now().isoformat(),
        }

    # All recipients combined for sending
    all_recipients = to_list + cc_list + bcc_list

    # Validate Excel result
    if not excel_result.get("success"):
        return {
            "success": False,
            "message": f"Excel file generation failed: {excel_result.get('message', 'Unknown error')}",
            "timestamp": datetime.now().isoformat(),
        }

    # Generate Excel download link
    s3_config = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region": s3_region,
    }

    excel_url = generate_excel_download_link(excel_result, s3_config)

    if not excel_url:
        return {
            "success": False,
            "message": "Failed to generate Excel download link",
            "timestamp": datetime.now().isoformat(),
        }

    # Create email content
    report_date = datetime.now().strftime("%d/%m/%Y")
    subject = f"📊 Báo cáo Excel thị trường chứng khoán - {report_date}"
    html_content, text_content = create_email_content_excel_only(merge_data, excel_url)

    # Send email using Gmail
    print(
        f"📧 Sending Excel report email to {len(all_recipients)} recipients via Gmail..."
    )

    result = send_email_gmail_smtp(
        gmail_user, gmail_password, all_recipients, subject, html_content, text_content
    )

    # Add additional metadata to result
    result["recipients"] = {
        "to": to_list,
        "cc": cc_list,
        "bcc": bcc_list,
        "total": len(all_recipients),
    }
    result["excel_download_link"] = excel_url
    result["timestamp"] = datetime.now().isoformat()
    result["report_date"] = report_date

    # Enhanced success logging
    if result["success"]:
        print(
            f"✅ Excel report email sent successfully to {len(all_recipients)} recipients"
        )
        print(f"📊 Excel download link: {excel_url[:50]}...")
    else:
        print(f"❌ Failed to send Excel report email: {result['message']}")

    return result


# Example usage function
def example_usage():
    """Example showing how to use the Excel-only Gmail notifier"""

    # Sample Excel result (would come from your Excel generation step)
    sample_excel_result = {
        "success": True,
        "message": "Excel report generated successfully",
        "s3_location": "s3://your-bucket/reports/stock_summary_20250721.xlsx",
        "file_size_bytes": 25600,
        "timestamp": datetime.now().isoformat(),
    }

    # Sample merge data (would come from your data processing step)
    sample_merge_data = {
        "khoi_ngoai": {"vol": 6.66, "net_value": -83.31},
        "khoi_tu_doanh": 143.49,
        "top_sectors": ["Nguyên vật liệu", "Tiện ích Cộng đồng", "Dược phẩm"],
        "top_interested": ["SSI", "CEO", "VIX", "HPG", "VPB"],
        "index_summary": [{"indexId": "VN30"}, {"indexId": "VNINDEX"}],
        "execution_metadata": {"success_rate": 1.0, "total_modules": 7},
    }

    # Example function call
    result = main(
        excel_result=sample_excel_result,
        merge_data=sample_merge_data,
        to_emails="recipient1@example.com,recipient2@example.com",
        cc_emails="cc@example.com",
        gmail_user="your-gmail@gmail.com",
        gmail_password="your-app-password",
        aws_access_key_id="your-aws-key",
        aws_secret_access_key="your-aws-secret",
        s3_region="ap-southeast-2",
    )

    print("Email sending result:", result)
    return result
