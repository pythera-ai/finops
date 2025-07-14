import wmill
import os
import csv
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Dict, List, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Constants for icons ---
UP_ARROW = "↑"
DOWN_ARROW = "↓"
NO_CHANGE_ICON = "↔"


# --- Configuration ---
class Config:
    """Configuration class for better organization"""

    def __init__(
        self,
        s3_bucket_name: str = "ragbucket.hungnq",
        s3_csv_key: str = "stock_summary_report.csv",
        s3_region: str = "ap-southeast-2",
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ):
        self.S3_BUCKET_NAME = s3_bucket_name
        self.S3_CSV_KEY = s3_csv_key
        self.S3_REGION = s3_region
        self.AWS_ACCESS_KEY_ID = aws_access_key_id
        self.AWS_SECRET_ACCESS_KEY = aws_secret_access_key

        # Report configuration
        self.REPORT_TITLE_TEMPLATE = "TỔNG KẾT GIAO DỊCH PHIÊN {date}"
        self.DATE_FORMAT = "%d/%m/%Y"


class StockReportGenerator:
    """Main class for generating stock market reports"""

    def __init__(self, config: Config):
        self.config = config
        self.s3_client = self._initialize_s3_client()

        # Index mapping for consistent labeling
        self.index_mapping = {
            "VNINDEX": "VNINDEX",
            "VN30": "VN30",
            "HNXIndex": "HNXIndex",
            "HNX30": "HNX30",
            "HNXUpcomIndex": "UPCOM",
            "VNXALL": "VNXALL",
        }

        # CSV headers
        self.csv_headers = [
            "CHỈ SỐ",
            "ĐIỂM",
            "(+/-)",
            "(+/-)%",
            "KLGD (triệu cp)",
            "GTGD (tỷ)",
            "CP tăng/giảm",
        ]

    def _initialize_s3_client(self) -> Optional[boto3.client]:
        """Initialize S3 client with proper error handling"""
        try:
            if self.config.AWS_ACCESS_KEY_ID and self.config.AWS_SECRET_ACCESS_KEY:
                return boto3.client(
                    "s3",
                    region_name=self.config.S3_REGION,
                    aws_access_key_id=self.config.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=self.config.AWS_SECRET_ACCESS_KEY,
                )
            else:
                # Use default credentials (IAM role, AWS CLI config, etc.)
                return boto3.client("s3", region_name=self.config.S3_REGION)
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            return None

    @staticmethod
    def format_value_with_icon(value: float, decimals: int = 2, unit: str = "") -> str:
        """Format values with appropriate icons and signs"""
        if value == 0:
            icon = NO_CHANGE_ICON
            sign = ""
        elif value > 0:
            icon = UP_ARROW
            sign = "+"
        else:
            icon = DOWN_ARROW
            sign = ""  # Negative sign is inherent

        return f"{icon} {sign}{value:.{decimals}f} {unit}".strip()

    def validate_input_data(self, data: Dict[str, Any]) -> bool:
        """Validate input data structure"""
        required_keys = [
            "index_summary",
            "khoi_ngoai",
            "top_netforeign",
            "khoi_tu_doanh",
            "top_sectors",
            "top_interested",
            "impact_up",
            "impact_down",
        ]

        for key in required_keys:
            if key not in data:
                logger.error(f"Missing required key: {key}")
                return False

        # Validate index_summary structure
        if not isinstance(data["index_summary"], list):
            logger.error("index_summary must be a list")
            return False

        # Validate khoi_ngoai structure
        kn_required = ["vol", "net_value"]
        for key in kn_required:
            if key not in data["khoi_ngoai"]:
                logger.error(f"Missing key in khoi_ngoai: {key}")
                return False

        return True

    def generate_index_rows(self, index_data: List[Dict[str, Any]]) -> List[List[str]]:
        """Generate rows for index summary"""
        rows = []
        index_dict = {item["indexId"]: item for item in index_data}

        for data_label, display_label in self.index_mapping.items():
            item = index_dict.get(data_label)

            if item:
                try:
                    row = [
                        display_label,
                        f"{float(item['indexValue']):.2f}",
                        self.format_value_with_icon(float(item["change"]), 2),
                        self.format_value_with_icon(float(item["changePercent"]), 2),
                        f"{float(item['allQty']):.1f}",
                        f"{float(item['allValue']):.1f}",
                        f"{UP_ARROW}{item['advances']}|{NO_CHANGE_ICON}{item['nochanges']}|{DOWN_ARROW}{item['declines']}",
                    ]
                    rows.append(row)
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Error processing index {data_label}: {e}")
                    rows.append([display_label] + ["N/A"] * 6)
            else:
                logger.warning(f"Index {data_label} not found in data")
                rows.append([display_label] + ["N/A"] * 6)

        return rows

    def generate_csv_content(self, data: Dict[str, Any]) -> List[List[str]]:
        """Generate complete CSV content"""
        csv_rows = []
        report_date = datetime.now().strftime(self.config.DATE_FORMAT)

        # Title
        title = self.config.REPORT_TITLE_TEMPLATE.format(date=report_date)
        csv_rows.append([title])

        # Headers
        csv_rows.append(self.csv_headers)

        # Index data
        csv_rows.extend(self.generate_index_rows(data["index_summary"]))
        csv_rows.append([])  # Empty row for spacing

        # Khối ngoại
        try:
            kn_data = data["khoi_ngoai"]
            kn_vol = self.format_value_with_icon(float(kn_data["vol"]), 2, "triệu cp")
            kn_net_value = self.format_value_with_icon(
                float(kn_data["net_value"]), 2, "tỷ đồng"
            )
            csv_rows.append(["Khối ngoại:", kn_vol, kn_net_value])
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing khoi_ngoai data: {e}")
            csv_rows.append(["Khối ngoại:", "N/A", "N/A"])

        # Top mua/bán ròng
        try:
            top_net = data["top_netforeign"]
            csv_rows.append(["Top mua ròng:", ",".join(top_net.get("buy", []))])
            csv_rows.append(["Top bán ròng:", ",".join(top_net.get("sell", []))])
        except (KeyError, TypeError) as e:
            logger.error(f"Error processing top_netforeign data: {e}")
            csv_rows.append(["Top mua ròng:", "N/A"])
            csv_rows.append(["Top bán ròng:", "N/A"])

        # Khối tự doanh
        try:
            ktd_value = float(data["khoi_tu_doanh"])
            ktd_formatted = self.format_value_with_icon(ktd_value, 2, "tỷ đồng")
            csv_rows.append(["Khối tự doanh:", ktd_formatted])
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing khoi_tu_doanh data: {e}")
            csv_rows.append(["Khối tự doanh:", "N/A"])

        # Nhóm ngành nổi bật
        csv_rows.append(["Nhóm ngành nổi bật:", ",".join(data.get("top_sectors", []))])

        # Cổ phiếu tâm điểm
        csv_rows.append(
            ["Cổ phiếu tâm điểm:", ",".join(data.get("top_interested", []))]
        )

        # Tác động tăng/giảm
        try:
            impact_up = data["impact_up"]
            csv_rows.append(
                [
                    f"Tác động tăng (+{float(impact_up['total']):.2f}):",
                    ",".join(impact_up.get("stock_code", [])),
                ]
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing impact_up data: {e}")
            csv_rows.append(["Tác động tăng:", "N/A"])

        try:
            impact_down = data["impact_down"]
            csv_rows.append(
                [
                    f"Tác động giảm ({float(impact_down['total']):.2f}):",
                    ",".join(impact_down.get("stock_code", [])),
                ]
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing impact_down data: {e}")
            csv_rows.append(["Tác động giảm:", "N/A"])

        return csv_rows

    def write_csv_file(self, csv_rows: List[List[str]], file_path: str) -> bool:
        """Write CSV data to file"""
        try:
            with open(file_path, "w", newline="", encoding="utf-8-sig") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(csv_rows)
            logger.info(f"CSV file '{file_path}' created successfully")
            return True
        except IOError as e:
            logger.error(f"Error writing CSV file: {e}")
            return False

    def upload_to_s3(self, local_file_path: str) -> bool:
        """Upload file to S3 with comprehensive error handling"""
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False

        if not os.path.exists(local_file_path):
            logger.error(f"File not found: {local_file_path}")
            return False

        try:
            self.s3_client.upload_file(
                local_file_path, self.config.S3_BUCKET_NAME, self.config.S3_CSV_KEY
            )
            logger.info(
                f"Successfully uploaded {local_file_path} to "
                f"s3://{self.config.S3_BUCKET_NAME}/{self.config.S3_CSV_KEY}"
            )
            return True

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "NoSuchBucket":
                logger.error(f"S3 bucket '{self.config.S3_BUCKET_NAME}' does not exist")
            elif error_code == "AccessDenied":
                logger.error("Access denied to S3 bucket")
            else:
                logger.error(f"S3 ClientError: {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error during S3 upload: {e}")
            return False

    def generate_report(self, data: Dict[str, Any]) -> bool:
        """Main method to generate and upload report"""
        try:
            # Validate input data
            if not self.validate_input_data(data):
                logger.error("Input data validation failed")
                return False

            # Generate CSV content
            csv_rows = self.generate_csv_content(data)

            # Write to file
            if not self.write_csv_file(csv_rows, self.config.S3_CSV_KEY):
                return False

            # Upload to S3
            return self.upload_to_s3(self.config.S3_CSV_KEY)

        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return False
        finally:
            # Cleanup local file
            if os.path.exists(self.config.S3_CSV_KEY):
                try:
                    os.remove(self.config.S3_CSV_KEY)
                    logger.info("Local CSV file cleaned up")
                except OSError as e:
                    logger.warning(f"Failed to cleanup local file: {e}")


# Windmill main function
def main(
    data: Dict[str, Any],
    s3_bucket_name: str = "ragbucket.hungnq",
    s3_csv_key: str = "stock_summary_report.csv",
    s3_region: str = "ap-southeast-2",
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
) -> Dict[str, Any]:
    """
    Main function for Windmill workflow

    Args:
        data: Input data containing stock market information
        s3_bucket_name: Name of the S3 bucket to upload to
        s3_csv_key: S3 key (path) for the CSV file
        s3_region: AWS region for S3 bucket
        aws_access_key_id: AWS access key ID (optional, will use IAM if not provided)
        aws_secret_access_key: AWS secret access key (optional, will use IAM if not provided)

    Returns:
        Dictionary with success status and message
    """
    try:
        # Create configuration from arguments
        config = Config(
            s3_bucket_name=s3_bucket_name,
            s3_csv_key=s3_csv_key,
            s3_region=s3_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        generator = StockReportGenerator(config)
        success = generator.generate_report(data)

        return {
            "success": success,
            "message": "Report generated and uploaded successfully"
            if success
            else "Report generation failed",
            "timestamp": datetime.now().isoformat(),
            "s3_location": f"s3://{config.S3_BUCKET_NAME}/{config.S3_CSV_KEY}"
            if success
            else None,
            "config_used": {
                "bucket": config.S3_BUCKET_NAME,
                "key": config.S3_CSV_KEY,
                "region": config.S3_REGION,
                "using_iam": aws_access_key_id is None,
            },
        }

    except Exception as e:
        logger.error(f"Unexpected error in main function: {e}")
        return {
            "success": False,
            "message": f"Unexpected error: {str(e)}",
            "timestamp": datetime.now().isoformat(),
            "s3_location": None,
        }
