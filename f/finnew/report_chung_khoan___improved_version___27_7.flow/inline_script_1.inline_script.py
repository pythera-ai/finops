import boto3
import os
import tempfile
from datetime import datetime

from playwright.sync_api import sync_playwright
# import imgkit


# Helper function to install browser if not present
def install_playwright_browser():
    """Ensures the playwright browser is installed."""
    import sys
    import subprocess

    print("Checking/installing Playwright browser...")
    # Running 'playwright install' from a script is tricky.
    # This command ensures it installs the default browser (chromium).
    _ = subprocess.run(
        [sys.executable, "-m", "playwright", "install-deps"],
        capture_output=True,
        text=True,
    )
    process2 = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        capture_output=True,
        text=True,
    )
    if process2.returncode != 0:
        print("Playwright install STDOUT:", process2.stdout)
        print("Playwright install STDERR:", process2.stderr)
        # raise RuntimeError("Failed to install Playwright browser.")
    print("Browser install check complete.")


def capture_screenshot(html_file_path, png_output_path):
    """Uses playwright to take a screenshot of a local HTML file."""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        # Use file:// protocol to open local file
        page.goto(f"file://{os.path.abspath(html_file_path)}")
        # Find the main report container and screenshot only that element
        # This gives a tight, clean crop of the report.
        report_element = page.locator(".report-container")
        report_element.screenshot(path=png_output_path)
        browser.close()


def main(
    s3_bucket_name: str,
    s3_html_key: str,
    s3_png_key: str,
    s3_region: str = "ap-southeast-2",
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
):
    """Main function to orchestrate download, screenshot, and upload."""
    # install_playwright_browser()

    try:
        s3_client = boto3.client(
            "s3",
            region_name=s3_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            local_html_path = os.path.join(temp_dir, "report.html")
            local_png_path = os.path.join(temp_dir, "report.png")

            # 1. Download HTML from S3
            print(f"Downloading s3://{s3_bucket_name}/{s3_html_key}...")
            s3_client.download_file(s3_bucket_name, s3_html_key, local_html_path)

            # 2. Capture Screenshot
            print("Capturing screenshot of HTML file...")
            capture_screenshot(local_html_path, local_png_path)
            # options = {
            #     "format": "png",
            #     "width": 820,  # Give it a little extra width to avoid wrapping
            #     "quality": 95,
            #     "encoding": "UTF-8",
            # }
            # imgkit.from_file(local_html_path, local_png_path, options=options)
            # print("Conversion complete.")
            print("Screenshot captured successfully.")

            # 3. Upload PNG to S3
            print(f"Uploading PNG to s3://{s3_bucket_name}/{s3_png_key}...")
            s3_client.upload_file(
                local_png_path,
                s3_bucket_name,
                s3_png_key,
                ExtraArgs={"ContentType": "image/png"},
            )
            file_size = os.path.getsize(local_png_path)

        return {
            "success": True,
            "message": "HTML successfully converted to PNG and uploaded.",
            "s3_png_location": f"s3://{s3_bucket_name}/{s3_png_key}",
            "file_size_bytes": file_size,
        }
    except Exception as e:
        # Add more detailed error logging
        import traceback

        print(f"An error occurred: {str(e)}")
        print(traceback.format_exc())
        return {"success": False, "message": f"HTML to PNG conversion failed: {str(e)}"}
