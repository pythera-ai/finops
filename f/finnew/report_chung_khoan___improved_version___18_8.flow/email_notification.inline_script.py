import boto3
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from datetime import datetime
from typing import Dict, Any
import logging
import requests
from io import BytesIO

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def send_email_gmail_smtp(
    gmail_user: str,
    gmail_password: str,
    to_emails: list,
    subject: str,
    html_body: str,
    text_body: str = None,
    image_data: bytes = None,
    image_cid: str = "report_image",
):
    """Send email using Gmail SMTP with inline image support"""
    try:
        msg = MIMEMultipart("related")
        msg["From"] = gmail_user
        msg["To"] = ", ".join(to_emails)
        msg["Subject"] = subject

        # Create alternative container for text and HTML
        msg_alternative = MIMEMultipart("alternative")

        if text_body:
            msg_alternative.attach(MIMEText(text_body, "plain"))
        msg_alternative.attach(MIMEText(html_body, "html"))

        # Attach the alternative container to the main message
        msg.attach(msg_alternative)

        # Attach inline image if provided
        if image_data:
            img = MIMEImage(image_data)
            img.add_header("Content-ID", f"<{image_cid}>")
            img.add_header("Content-Disposition", "inline", filename="report.png")
            msg.attach(img)

        context = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls(context=context)
            server.login(gmail_user, gmail_password)
            server.send_message(msg)
        return {"success": True, "message": "Email sent successfully via Gmail SMTP"}
    except Exception as e:
        logger.error(f"Gmail SMTP error: {str(e)}")
        return {"success": False, "message": f"Gmail SMTP error: {str(e)}"}


def download_image_from_s3(s3_client, s3_path):
    """Download image data from S3 path."""
    try:
        if not s3_path or not s3_path.startswith("s3://"):
            return None

        # Parse S3 path
        path_without_prefix = s3_path[5:]  # Remove "s3://"
        if "/" not in path_without_prefix:
            return None

        bucket, key = path_without_prefix.split("/", 1)
        if not bucket or not key:
            return None

        # Download image from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        image_data = response["Body"].read()

        logger.info(f"Successfully downloaded image from S3: {s3_path}")
        return image_data

    except Exception as e:
        logger.error(f"Error downloading image from S3: {e}")
        return None


def generate_presigned_s3_url(file_result, s3_client):
    """Generate a presigned S3 URL for a given file result (kept for backup download link)."""
    try:
        s3_location_key = (
            "s3_png_location" if "s3_png_location" in file_result else "s3_location"
        )

        if not file_result.get("success"):
            return ""

        s3_path = file_result.get(s3_location_key)
        if not s3_path or not s3_path.startswith("s3://"):
            return ""

        # Parse S3 path
        path_without_prefix = s3_path[5:]  # Remove "s3://"
        if "/" not in path_without_prefix:
            return ""

        bucket, key = path_without_prefix.split("/", 1)
        if not bucket or not key:
            return ""

        # Generate presigned URL
        file_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                "ResponseContentType": "image/png",
                "ResponseContentDisposition": f"attachment; filename=report_{datetime.now().strftime('%Y%m%d')}.png",
            },
            ExpiresIn=86400,  # 24 hours
        )

        return file_url

    except Exception as e:
        logger.error(f"Error generating presigned URL: {e}")
        return ""


def generate_vpbank_header_url(s3_client, bucket, key):
    """Generate presigned URL for VPBank header image."""
    try:
        # Check if object exists
        s3_client.head_object(Bucket=bucket, Key=key)

        # Generate presigned URL
        header_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=86400,  # 24 hours
        )
        return header_url
    except Exception as e:
        logger.warning(f"VPBank header image not found or accessible: {e}")
        return ""


def create_email_content_with_inline_image(
    merge_data, has_inline_image=False, backup_png_url="", vpbank_header_url=None
):
    """Create HTML and text email content with inline image display."""
    report_date = datetime.now().strftime("%d/%m/%Y")

    # Image section - inline display (without redundant title since it's in the image)
    if has_inline_image:
        image_section = f"""
            <div class="image-section">
                <div class="image-container">
                    <img src="cid:report_image" 
                         alt="Báo cáo thị trường chứng khoán {report_date}" 
                         style="width: 100%; max-width: 900px; height: auto; border: 1px solid #e9ecef; border-radius: 12px; box-shadow: 0 8px 16px rgba(0,0,0,0.1);">
                </div>
                {f'<div class="backup-download" style="text-align: center; margin-top: 20px;"><a href="{backup_png_url}" target="_blank" style="color: #10b981; text-decoration: none; font-size: 14px; padding: 8px 16px; border: 1px solid #10b981; border-radius: 6px; display: inline-block; transition: all 0.3s ease;">🔗 Tải xuống bản PNG chất lượng cao</a></div>' if backup_png_url else ""}
            </div>
        """
        text_image_info = "📊 Hình ảnh báo cáo được hiển thị trong email"
        if backup_png_url:
            text_image_info += f"\n🔗 Link tải xuống: {backup_png_url}"
    else:
        image_section = f"""
            <div class="image-section error">
                <h3 style="color: #d32f2f; margin-bottom: 15px;">❌ KHÔNG THỂ HIỂN THỊ BÁO CÁO</h3>
                <p style="color: #666; margin-bottom: 20px;">Báo cáo hình ảnh không khả dụng hoặc có lỗi xảy ra.</p>
                {f'<a href="{backup_png_url}" target="_blank" class="download-button">🔗 Thử tải xuống trực tiếp</a>' if backup_png_url else ""}
                <div style="color: #d32f2f; font-weight: bold; margin-top: 15px; padding: 15px; background: #fff5f5; border-radius: 8px;">
                    ⚠️ Vui lòng liên hệ bộ phận kỹ thuật để được hỗ trợ
                </div>
            </div>
        """
        text_image_info = "❌ Không thể hiển thị hình ảnh"
        if backup_png_url:
            text_image_info += f"\n🔗 Link tải xuống: {backup_png_url}"

    # Header content - Only show VPBank header if available, otherwise minimal header
    header_section = ""
    if vpbank_header_url:
        header_section = f'''
            <div class="header-image-section">
                <img src="{vpbank_header_url}" 
                     alt="VPBank Header" 
                     style="width: 100%; height: auto; max-height: 150px; object-fit: cover; display: block;">
            </div>
        '''
        logger.info("Using VPBank S3 header image")
    else:
        # Minimal header without redundant text
        header_section = """
            <div class="minimal-header">
                <div style="height: 8px; background: linear-gradient(90deg, #10b981 0%, #06b6d4 50%, #3b82f6 100%);"></div>
            </div>
        """

    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ 
                font-family: 'Times New Roman', serif; 
                line-height: 1.6; 
                color: #333; 
                margin: 0; 
                padding: 0; 
                background-color: #f8f9fa;
            }}
            .container {{
                max-width: 950px;
                margin: 0 auto;
                background: white;
                box-shadow: 0 0 25px rgba(0,0,0,0.08);
                border-radius: 8px;
                overflow: hidden;
            }}
            .header-image-section {{
                width: 100%;
                overflow: hidden;
            }}
            .minimal-header {{
                width: 100%;
            }}
            .content {{ 
                padding: 25px 15px; 
            }}
            .image-section {{ 
                background: #ffffff; 
                padding: 20px; 
                border-radius: 12px; 
                margin: 10px 0; 
                text-align: center; 
                border: 1px solid #f0f0f0;
            }}
            .image-section.error {{ 
                background: #fff8f8; 
                border-color: #fed7d7;
                padding: 30px 20px;
            }}
            .image-container {{
                margin: 0;
                text-align: center;
            }}
            .backup-download {{
                margin-top: 20px;
                padding-top: 20px;
                border-top: 1px solid #f0f0f0;
            }}
            .backup-download a:hover {{
                background: #10b981;
                color: white;
                text-decoration: none;
            }}
            .download-button {{ 
                display: inline-block; 
                padding: 15px 30px; 
                background: #dc3545; 
                color: white; 
                text-decoration: none; 
                border-radius: 8px; 
                font-weight: bold; 
                font-size: 14px; 
                transition: all 0.3s ease;
                margin-top: 15px;
            }}
            .download-button:hover {{ 
                background: #c82333;
                transform: translateY(-1px);
            }}
            .footer {{ 
                background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
                color: white;
                padding: 25px 20px; 
                text-align: center; 
                font-size: 13px;
            }}
            .footer p {{
                margin: 8px 0;
                opacity: 0.9;
            }}
            @media only screen and (max-width: 600px) {{
                .container {{
                    margin: 0;
                    box-shadow: none;
                    border-radius: 0;
                }}
                .content {{ 
                    padding: 20px 10px; 
                }}
                .image-section {{ 
                    margin: 10px 0; 
                    padding: 15px 10px; 
                    border-radius: 8px;
                }}
                .image-container img {{
                    border-radius: 8px;
                }}
                .footer {{
                    padding: 20px 15px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            {header_section}
            <div class="content">
                {image_section}
            </div>
            <div class="footer">
                <p>🤖 <strong>Báo cáo được tạo tự động bởi hệ thống Windmill</strong></p>
                <p>⏰ Thời gian tạo: {datetime.now().strftime("%H:%M:%S %d/%m/%Y")}</p>
                <p>📧 Email này được gửi tự động, vui lòng không trả lời trực tiếp</p>
            </div>
        </div>
    </body>
    </html>
    """

    # Plain text version
    text_body = f"""
    📈 BÁO CÁO THỊ TRƯỜNG CHỨNG KHOÁN - {report_date}
    ═══════════════════════════════════════════════════
    ⏰ THỜI GIAN TẠO: {datetime.now().strftime("%H:%M:%S %d/%m/%Y")}
    
    📊 HÌNH ẢNH BÁO CÁO:
    {text_image_info}
    
    ═══════════════════════════════════════════════════
    🤖 Báo cáo được tạo tự động bởi hệ thống Windmill
    📧 Email này được gửi tự động, vui lòng không trả lời trực tiếp
    """

    return html_body, text_body


def main(
    image_result: Dict[str, Any],
    merge_data: Dict[str, Any],
    to_emails: str,
    cc_emails: str = "",
    bcc_emails: str = "",
    gmail_user: str = "",
    gmail_password: str = "",
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    s3_region: str = "ap-southeast-2",
    vpbank_header_bucket: str = None,
    vpbank_header_key: str = None,
) -> Dict[str, Any]:
    """Send Gmail notification with inline PNG image display and VPBank header from S3."""

    # Validate inputs
    if not gmail_user or not gmail_password:
        return {"success": False, "message": "Gmail credentials are required"}

    to_list = [email.strip() for email in to_emails.split(",") if email.strip()]
    if not to_list:
        return {"success": False, "message": "No recipient emails provided"}

    all_recipients = (
        to_list
        + [e.strip() for e in (cc_emails or "").split(",") if e.strip()]
        + [e.strip() for e in (bcc_emails or "").split(",") if e.strip()]
    )

    if not image_result.get("success"):
        return {
            "success": False,
            "message": f"Image generation failed: {image_result.get('message', 'Unknown error')}",
        }

    # Create S3 client
    s3_client = boto3.client(
        "s3",
        region_name=s3_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # Download image from S3 for inline attachment
    s3_location_key = (
        "s3_png_location" if "s3_png_location" in image_result else "s3_location"
    )
    s3_path = image_result.get(s3_location_key)

    image_data = None
    if s3_path:
        image_data = download_image_from_s3(s3_client, s3_path)

    # Generate backup download link
    backup_png_url = generate_presigned_s3_url(image_result, s3_client)

    # Generate VPBank header URL if provided
    vpbank_header_url = None
    if vpbank_header_bucket and vpbank_header_key:
        vpbank_header_url = generate_vpbank_header_url(
            s3_client, vpbank_header_bucket, vpbank_header_key
        )

    # Create email content
    report_date = datetime.now().strftime("%d/%m/%Y")
    subject = f"📊 Báo cáo Hình ảnh Thị trường Chứng khoán - {report_date}"
    html_content, text_content = create_email_content_with_inline_image(
        merge_data,
        has_inline_image=bool(image_data),
        backup_png_url=backup_png_url,
        vpbank_header_url=vpbank_header_url,
    )

    # Send email with inline image
    result = send_email_gmail_smtp(
        gmail_user,
        gmail_password,
        all_recipients,
        subject,
        html_content,
        text_content,
        image_data=image_data,
        image_cid="report_image",
    )

    # Add metadata to result
    result["recipients"] = {
        "to": to_list,
        "cc": [e.strip() for e in (cc_emails or "").split(",") if e.strip()],
        "bcc": [e.strip() for e in (bcc_emails or "").split(",") if e.strip()],
        "total": len(all_recipients),
    }
    result["inline_image_attached"] = bool(image_data)
    result["backup_download_link"] = backup_png_url
    result["vpbank_header_used"] = bool(vpbank_header_url)
    result["timestamp"] = datetime.now().isoformat()

    if image_data:
        result["image_size_kb"] = round(len(image_data) / 1024, 2)

    logger.info(f"Email sent with inline image: {bool(image_data)}")

    return result
