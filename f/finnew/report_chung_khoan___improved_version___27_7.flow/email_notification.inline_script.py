import boto3
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, Any
import logging

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
        context = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls(context=context)
            server.login(gmail_user, gmail_password)
            server.send_message(msg)
        return {"success": True, "message": "Email sent successfully via Gmail SMTP"}
    except Exception as e:
        logger.error(f"Gmail SMTP error: {str(e)}")
        return {"success": False, "message": f"Gmail SMTP error: {str(e)}"}


def generate_presigned_s3_url(file_result, s3_config):
    """Generate a presigned S3 URL for a given file result with better error handling."""
    file_url = ""
    try:
        # Validate S3 config
        required_keys = ["aws_access_key_id", "aws_secret_access_key"]
        for key in required_keys:
            if not s3_config.get(key):
                logger.error(f"Missing S3 configuration: {key}")
                return ""

        s3_client = boto3.client(
            "s3",
            region_name=s3_config.get("region", "ap-southeast-2"),
            aws_access_key_id=s3_config.get("aws_access_key_id"),
            aws_secret_access_key=s3_config.get("aws_secret_access_key"),
        )

        # Determine the S3 location key
        s3_location_key = (
            "s3_png_location" if "s3_png_location" in file_result else "s3_location"
        )

        if not file_result.get("success"):
            logger.error("File result indicates failure")
            return ""

        s3_path = file_result.get(s3_location_key)
        if not s3_path:
            logger.error(
                f"No S3 path found in result. Available keys: {list(file_result.keys())}"
            )
            return ""

        logger.info(f"Processing S3 path: {s3_path}")

        # FIXED: Properly parse S3 path
        if not s3_path.startswith("s3://"):
            logger.error(f"Invalid S3 path format: {s3_path}")
            return ""

        # Remove s3:// prefix and split into bucket and key
        path_without_prefix = s3_path[5:]  # Remove "s3://"
        if "/" not in path_without_prefix:
            logger.error(f"Invalid S3 path - no key found: {s3_path}")
            return ""

        # Split into bucket and key (only split on first "/")
        bucket, key = path_without_prefix.split("/", 1)

        if not bucket or not key:
            logger.error(
                f"Invalid bucket ({bucket}) or key ({key}) from path: {s3_path}"
            )
            return ""

        logger.info(f"Bucket: {bucket}, Key: {key}")

        # Check if object exists
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            logger.info("S3 object exists and is accessible")
        except Exception as e:
            logger.error(f"S3 object not accessible: {e}")
            return ""

        # Generate presigned URL with longer expiration and proper content type
        file_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                "ResponseContentType": "image/png",  # Ensure proper content type
                "ResponseContentDisposition": f"attachment; filename=report_{datetime.now().strftime('%Y%m%d')}.png",
            },
            ExpiresIn=86400,  # 24 hours
        )

        logger.info(f"Generated presigned URL successfully: {file_url[:60]}...")

    except Exception as e:
        logger.error(f"Error generating presigned URL: {e}")
        return ""

    return file_url


def create_email_content_with_png_link(merge_data, png_url):
    """Create HTML and text email content for a PNG file download link."""
    report_date = datetime.now().strftime("%d/%m/%Y")

    # HTML body with better error handling
    if png_url:
        download_section = f'''
            <div class="download-section">
                <h3>🖼️ TẢI XUỐNG BÁO CÁO HÌNH ẢNH</h3>
                <p>Bản tóm tắt thị trường trực quan, dễ theo dõi.</p>
                <a href="{png_url}" target="_blank" class="download-button" download>
                    <span class="image-icon">🖼️</span>TẢI XUỐNG HÌNH ẢNH
                </a>
                <p style="font-size: 12px; color: #666; margin-top: 10px;">
                    * Liên kết tải xuống có hiệu lực trong 24 giờ<br>
                    * Click vào nút để tải xuống trực tiếp
                </p>
            </div>
        '''
        text_download = f"🖼️ Hình ảnh: {png_url}"
    else:
        download_section = """
            <div class="download-section" style="background: #ffe6e6;">
                <h3>❌ KHÔNG THỂ TẢI XUỐNG</h3>
                <p>Báo cáo hình ảnh không khả dụng hoặc có lỗi xảy ra.</p>
                <div style="color: #d32f2f; font-weight: bold;">
                    Vui lòng liên hệ bộ phận kỹ thuật để được hỗ trợ.
                </div>
            </div>
        """
        text_download = "🖼️ Hình ảnh: Không khả dụng"

    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: 'Times New Roman', serif; line-height: 1.6; color: #333; }}
            .header {{ background: linear-gradient(135deg, #4CAF50 0%, #26A69A 100%); color: white; padding: 20px; text-align: center; }}
            .content {{ padding: 20px; }}
            .download-section {{ background: #e8f5e8; padding: 20px; border-radius: 10px; margin: 20px 0; text-align: center; }}
            .download-button {{ display: inline-block; padding: 15px 30px; background: #4CAF50; color: white; text-decoration: none; border-radius: 8px; font-weight: bold; font-size: 16px; }}
            .download-button:hover {{ background: #45a049; }}
            .image-icon {{ font-size: 24px; margin-right: 10px; }}
            .footer {{ background: #f8f9fa; padding: 15px; text-align: center; font-size: 12px; color: #666; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 BÁO CÁO THỊ TRƯỜNG CHỨNG KHOÁN</h1>
            <h2>{report_date}</h2>
        </div>
        <div class="content">
            {download_section}
        </div>
        <div class="footer">
            <p>🤖 Báo cáo được tạo tự động bởi hệ thống Windmill</p>
            <p>Thời gian tạo: {datetime.now().strftime("%H:%M:%S %d/%m/%Y")}</p>
        </div>
    </body>
    </html>
    """

    # Plain text version
    text_body = f"""
    📊 BÁO CÁO THỊ TRƯỜNG CHỨNG KHOÁN - {report_date}
    ⏰ THỜI GIAN TẠO: {datetime.now().strftime("%H:%M:%S %d/%m/%Y")}
    
    📁 TẢI XUỐNG:
    {text_download}
    
    * Liên kết có hiệu lực trong 24 giờ
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
) -> Dict[str, Any]:
    """Send Gmail notification with a link to the generated PNG image."""

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

    # Check image result
    logger.info(f"Image result: {image_result}")

    if not image_result.get("success"):
        error_msg = (
            f"Image generation failed: {image_result.get('message', 'Unknown error')}"
        )
        logger.error(error_msg)
        return {"success": False, "message": error_msg}

    # Generate PNG download link with better error handling
    s3_config = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region": s3_region,
    }

    logger.info("Generating presigned S3 URL...")
    png_url = generate_presigned_s3_url(image_result, s3_config)

    if not png_url:
        error_msg = (
            "Failed to generate PNG download link - check S3 credentials and file path"
        )
        logger.error(error_msg)
        return {"success": False, "message": error_msg}

    # Create email content
    report_date = datetime.now().strftime("%d/%m/%Y")
    subject = f"📊 Báo cáo Hình ảnh Thị trường Chứng khoán - {report_date}"
    html_content, text_content = create_email_content_with_png_link(merge_data, png_url)

    # Send email
    logger.info(f"📧 Sending image report email to {len(all_recipients)} recipients...")
    result = send_email_gmail_smtp(
        gmail_user, gmail_password, all_recipients, subject, html_content, text_content
    )

    # Add metadata to the final result
    result["recipients"] = {
        "to": to_list,
        "cc": [e.strip() for e in (cc_emails or "").split(",") if e.strip()],
        "bcc": [e.strip() for e in (bcc_emails or "").split(",") if e.strip()],
        "total": len(all_recipients),
    }
    result["png_download_link"] = png_url
    result["timestamp"] = datetime.now().isoformat()
    result["image_result_debug"] = image_result  # For debugging

    if result["success"]:
        logger.info(f"✅ Image report email sent successfully. Link: {png_url[:60]}...")
    else:
        logger.error(f"❌ Failed to send image report email: {result['message']}")

    return result
