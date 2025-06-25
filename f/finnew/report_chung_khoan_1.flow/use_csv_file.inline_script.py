# import wmill
import json
import numpy as np
import csv
import boto3
from botocore.client import Config
from datetime import datetime
from botocore.exceptions import ClientError

# --- Constants for icons ---
UP_ARROW = "↑"
DOWN_ARROW = "↓"
NO_CHANGE_ICON = "↔"

# --- S3 Configuration ---
# !!! IMPORTANT: REPLACE with your actual bucket names and keys !!!
S3_BUCKET_NAME = (
    "ragbucket.hungnq"  # Bucket for both CSV and Image, or can be different
)
S3_CSV_KEY = "stock_summary_report.csv"  # Key (path in S3) for the input CSV
# Optional: Specify the AWS region if your bucket is not in your default configured region
S3_REGION = None  # e.g., "us-east-1", "ap-southeast-1". If None, uses default.


# --- Formatting Functions ---
def fmt_chg(value, decimals=2, suffix=""):
    """Formats change values with icon and sign for table cells (+/-) and (+/- %)."""
    icon = UP_ARROW if value > 0 else (DOWN_ARROW if value < 0 else NO_CHANGE_ICON)
    sign = "+" if value > 0 else ""  # Negative sign is inherent in the value
    return f"{icon} {sign}{value:.{decimals}f}{suffix}"


def fmt_val_icon_paren(value, decimals=2, unit=""):
    """Formats values with icon, parentheses for Khoi Ngoai Volume, Khoi Tu Doanh."""
    icon = UP_ARROW if value > 0 else (DOWN_ARROW if value < 0 else NO_CHANGE_ICON)
    return f"{icon} ({value:.{decimals}f}) {unit}"


def fmt_val_icon(value, decimals=2, unit=""):
    """Formats values with icon for Khoi Ngoai Net Value."""
    icon = UP_ARROW if value > 0 else (DOWN_ARROW if value < 0 else NO_CHANGE_ICON)
    # Add + for positive values that are not zero; negative sign is inherent
    sign = "+" if value > 0 and icon != NO_CHANGE_ICON else ""
    return f"{icon} {sign}{value:.{decimals}f} {unit}"


# --- S3 Upload Function ---
def upload_to_s3(
    local_file_path, bucket_name, s3_object_key, region_name="ap-southeast-2"
):
    """
    Uploads a file to an S3 bucket.

    :param local_file_path: Path to the file to upload.
    :param bucket_name: Name of the S3 bucket.
    :param s3_object_key: The desired key (filename including path) in S3.
    :param region_name: AWS region of the bucket. If None, uses default from config.
    :return: True if upload was successful, False otherwise.
    """
    if region_name:
        s3_client = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id="",
            aws_secret_access_key="",
        )

    else:
        s3_client = boto3.client("s3")  # Uses default region from config

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_object_key)
        print(
            f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{s3_object_key}"
        )
        return True
    except FileNotFoundError:
        print(f"Error: The file {local_file_path} was not found.")
        return False
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        return False
    except Exception as e:  # Catch-all for other unexpected errors
        print(f"An unexpected error occurred during S3 upload: {e}")
        return False


def main(data: dict):
    # --- Prepare data for CSV ---
    csv_rows = []
    report_date = datetime.now().strftime("%d/%m/%Y")

    # Title (using date from image as requested for similarity)
    csv_rows.append([f"TỔNG KẾT GIAO DỊCH PHIÊN {report_date}"])
    csv_rows.append([])  # Empty row for spacing

    # Table Headers
    headers = [
        "CHỈ SỐ",
        "ĐIỂM",
        "(+/-)",
        "KLGD (triệu cp)",
        "GTGD (tỷ)",
        "CP tăng/giảm",
    ]
    csv_rows.append(headers)

    # Map input labels to image labels for easier lookup
    # Image labels: VnIndex, Vn30, HnxIndex, Hnx30, Upcom
    # Input data labels: VNINDEX, VN30, HNX, HNX30, UPCOM
    label_mapping_img_to_data = {
        "VNINDEX": "VNINDEX",
        "VN30": "VN30",
        "HNXIndex": "HNXIndex",
        "HNX30": "HNX30",
        "HNXUpcomIndex": "UPCOM",
        "VNXALL": "VNXALL",
    }
    # Create a dictionary from the list for quick lookup by data label
    index_summary_dict = {item["indexId"]: item for item in data["index_summary"]}

    for data_label in label_mapping_img_to_data.keys():
        # data_label = label_mapping_img_to_data.get(img_label)
        item = index_summary_dict.get(data_label)

        if item:
            # Use .2f for ĐIỂM, (+/-)
            # Use .2f for (+/- %)
            # Use .1f for KLGD, GTGD
            row = [
                label_mapping_img_to_data.get(data_label),
                f"{item['indexValue']:.2f}",
                fmt_chg(item["change"], decimals=2),
                f"{item['allQty']:.1f}",  # KLGD (already in millions from allQty)
                f"{item['allValue']:.1f}",  # GTGD (already in billions from allValue)
                f"{UP_ARROW}{item['advances']}|{NO_CHANGE_ICON}{item['nochanges']}|{DOWN_ARROW}{item['declines']}",
            ]
            csv_rows.append(row)
        else:
            # Handle cases where an expected index might be missing in data
            csv_rows.append(
                [
                    label_mapping_img_to_data(data_label),
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                ]
            )

    csv_rows.append([])  # Empty row for spacing

    # Khối ngoại
    kn_data = data["khoi_ngoai"]
    # For vol: from image (-15.66) triệu cp. Input data: -19.865065
    kn_vol_formatted = fmt_val_icon_paren(kn_data["vol"], 2, "triệu cp")
    # For net_value: from image 92.91 tỷ đồng. Input data: -341.36067205
    kn_net_value_formatted = fmt_val_icon(kn_data["net_value"], 2, "tỷ đồng")
    # To somewhat replicate the image layout (Label: Vol, then Net Value further right)
    # Structure: [Label, Vol Info, Empty Cell, Net Value Info] to create a visual separation.
    csv_rows.append(["Khối ngoại:", kn_vol_formatted, "", kn_net_value_formatted])

    # Top mua ròng / bán ròng
    # For these, the label is in the first column, and the list of stocks in the second.
    # If the list of stocks is long, it will extend the second column.
    csv_rows.append(["Top mua ròng:", ", ".join(data["top_netforeign"]["buy"])])
    csv_rows.append(["Top bán ròng:", ", ".join(data["top_netforeign"]["sell"])])

    # Khối tự doanh
    # From image: (-245 tỷ đồng). Input data: -451.63369
    ktd_value = data["khoi_tu_doanh"]
    ktd_formatted = fmt_val_icon_paren(ktd_value, 2, "tỷ đồng")
    csv_rows.append(["Khối tự doanh:", ktd_formatted])

    # Nhóm ngành nổi bật
    csv_rows.append(["Nhóm ngành nổi bật:", ", ".join(data["top_sectors"])])

    # Cổ phiếu tâm điểm
    csv_rows.append(["Cổ phiếu tâm điểm:", ", ".join(data["top_interested"])])

    # csv_file_path = "stock_summary_report.csv"
    csv_file_path = S3_CSV_KEY
    # Use utf-8-sig for better Excel compatibility with UTF-8 characters (like arrows)
    with open(csv_file_path, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile)
        for row_data in csv_rows:
            writer.writerow(row_data)

    print(f"CSV file '{csv_file_path}' has been created successfully.")
    return upload_to_s3(csv_file_path, S3_BUCKET_NAME, S3_CSV_KEY)
