import boto3
import csv
import os
import logging
import time
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Dict, List, Any, Optional
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedStockReportGenerator:
    def __init__(self, s3_config: dict):
        self.s3_config = s3_config
        self.s3_client = self._initialize_s3_client()

        # Enhanced index mapping - excluding VNXALL to match Excel format
        self.index_mapping = {
            "HNX30": "HNX30",
            "HNXIndex": "HNXINDEX",
            "HNXUpcomIndex": "UPCOM",
            "VN30": "VN30",
            "VNINDEX": "VNINDEX",
            # VNXALL excluded to match the Excel format
        }

        # Updated CSV headers to match the new format
        self.csv_headers = [
            "CHỈ SỐ",
            "ĐIỂM",
            "(+/-)",
            "(+/-)%",
            "KLGD (triệu cp)",
            "(+/-%KLGD)",  # NEW COLUMN
            "GTGD (tỷ)",
            "(+/-%GTGD)",  # NEW COLUMN
            "CP tăng/giảm",
        ]

    def _initialize_s3_client(self) -> Optional[boto3.client]:
        try:
            return boto3.client(
                "s3",
                region_name=self.s3_config.get("region", "ap-southeast-2"),
                aws_access_key_id=self.s3_config.get("access_key_id"),
                aws_secret_access_key=self.s3_config.get("secret_access_key"),
            )
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            return None

    def validate_input_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced data validation with detailed reporting"""
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "data_completeness": {},
        }

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
                validation_result["errors"].append(f"Missing required key: {key}")
                validation_result["valid"] = False
                validation_result["data_completeness"][key] = 0
            elif not data[key]:
                validation_result["warnings"].append(f"Empty data for key: {key}")
                validation_result["data_completeness"][key] = 0
            else:
                # Calculate completeness score
                if key == "index_summary":
                    validation_result["data_completeness"][key] = len(data[key])
                elif key in ["top_netforeign"]:
                    buy_count = len(data[key].get("buy", []))
                    sell_count = len(data[key].get("sell", []))
                    validation_result["data_completeness"][key] = (
                        buy_count + sell_count
                    ) / 2
                elif isinstance(data[key], list):
                    validation_result["data_completeness"][key] = len(data[key])
                else:
                    validation_result["data_completeness"][key] = 1

        return validation_result

    def generate_csv_content_with_metadata(
        self, data: Dict[str, Any]
    ) -> List[List[str]]:
        """Generate CSV content with metadata and quality information"""
        csv_rows = []
        report_date = datetime.now().strftime("%d/%m/%Y")

        # Enhanced title with quality indicator
        execution_meta = data.get("execution_metadata", {})
        quality_meta = data.get("data_quality", {})

        title = f"TỔNG KẾT GIAO DỊCH PHIÊN {report_date}"
        if not quality_meta.get("passed", True):
            title += " (CHẤT LƯỢNG DỮ LIỆU CẢNH BÁO)"

        csv_rows.append([title])

        # Add metadata row
        metadata_info = f"Tỷ lệ thành công: {execution_meta.get('success_rate', 0) * 100:.1f}% | Thời gian: {datetime.now().strftime('%H:%M:%S')}"
        csv_rows.append([metadata_info])
        csv_rows.append([])  # Empty row

        # Headers
        csv_rows.append(self.csv_headers)

        # Index data with error handling
        index_data = data.get("index_summary", [])
        if index_data:
            csv_rows.extend(self._generate_index_rows_with_percentages(index_data))
        else:
            csv_rows.append(["No index data available"])

        csv_rows.append([])  # Empty row

        # Enhanced sections with fallback values
        self._add_enhanced_sections(csv_rows, data)

        # Add quality report at the end
        if quality_meta.get("warnings") or quality_meta.get("issues"):
            csv_rows.append([])
            csv_rows.append(["=== BÁO CÁO CHẤT LƯỢNG DỮ LIỆU ==="])

            if quality_meta.get("issues"):
                csv_rows.append(["Vấn đề:", ", ".join(quality_meta["issues"])])

            if quality_meta.get("warnings"):
                csv_rows.append(["Cảnh báo:", ", ".join(quality_meta["warnings"])])

        return csv_rows

    def _generate_index_rows_with_percentages(
        self, index_data: List[Dict[str, Any]]
    ) -> List[List[str]]:
        """Generate index rows with percentage columns for KLGD and GTGD"""
        rows = []
        index_dict = {item.get("indexId", "Unknown"): item for item in index_data}

        for data_label, display_label in self.index_mapping.items():
            item = index_dict.get(data_label)

            if item:
                try:
                    row = [
                        display_label,
                        self._safe_float_format(item.get("indexValue")),
                        self._format_with_arrow(item.get("change", 0)),
                        self._format_with_arrow_percent(item.get("changePercent", 0)),
                        self._safe_float_format(item.get("allQty")),
                        # NEW: KLGD percentage column
                        self._format_with_arrow_percent(
                            item.get("klgd_change_percent", 0)
                        ),
                        self._safe_float_format(item.get("allValue")),
                        # NEW: GTGD percentage column
                        self._format_with_arrow_percent(
                            item.get("gtdg_change_percent", 0)
                        ),
                        self._format_stock_changes(item),
                    ]
                    rows.append(row)
                except Exception as e:
                    logger.warning(f"Error processing index {data_label}: {e}")
                    rows.append([display_label] + ["N/A"] * 8)  # Updated to 8 columns
            else:
                rows.append([display_label] + ["N/A"] * 8)  # Updated to 8 columns

        return rows

    def _safe_float_format(self, value) -> str:
        """Safely format float values"""
        if value == "N/A" or value is None:
            return "N/A"
        val = float(value)
        return f"{val:.2f}"

    def _format_with_arrow(self, value) -> str:
        """Format values with directional arrows"""
        try:
            val = float(value)
            if val == 0:
                return "↔0.00"
            elif val > 0:
                return f"↑{val:.2f}"
            else:
                return f"↓{abs(val):.2f}"
        except (ValueError, TypeError):
            return "N/A"

    def _format_with_arrow_percent(self, value) -> str:
        """Format percentage values with directional arrows"""
        try:
            val = float(value)
            if val == 0:
                return "↔0.00"
            elif val > 0:
                return f"↑{val:.2f}"
            else:
                return f"↓{abs(val):.2f}"
        except (ValueError, TypeError):
            return "N/A"

    def _format_percentage_change(self, value) -> str:
        """Format percentage change values for KLGD and GTGD columns"""
        try:
            val = float(value)
            if val == 0:
                return "0.0%"
            elif val > 0:
                return f"{val:.2f}%"
            else:
                return f"{val:.2f}%"
        except (ValueError, TypeError):
            return "N/A"

    def _format_stock_changes(self, item: dict) -> str:
        """Format stock changes with safe handling"""
        try:
            advances = item.get("advances", 0)
            nochanges = item.get("nochanges", 0)
            declines = item.get("declines", 0)
            return f"↑{advances}|↔{nochanges}|↓{declines}"
        except:
            return "N/A"

    def _add_enhanced_sections(self, csv_rows: List[List[str]], data: Dict[str, Any]):
        """Add enhanced sections with fallback handling"""

        # Khối ngoại
        kn_data = data.get("khoi_ngoai", {})
        if kn_data:
            vol = kn_data.get("vol", 0)
            net_value = kn_data.get("net_value", 0)

            vol_text = f"↑{vol:.2f}" if vol >= 0 else f"↓{abs(vol):.2f}"
            net_text = (
                f"↑{net_value:.2f}" if net_value >= 0 else f"↓{abs(net_value):.2f}"
            )

            csv_rows.append([f"Khối ngoại: {vol_text} triệu cp {net_text} tỷ đồng"])
        else:
            csv_rows.append(["Khối ngoại: N/A"])

        # Top mua/bán ròng
        top_net = data.get("top_netforeign", {})
        buy_stocks = top_net.get("buy", [])
        sell_stocks = top_net.get("sell", [])

        csv_rows.append(
            [f"Top mua ròng: {', '.join(buy_stocks) if buy_stocks else 'N/A'}"]
        )
        csv_rows.append(
            [f"Top bán ròng: {', '.join(sell_stocks) if sell_stocks else 'N/A'}"]
        )

        # Khối tự doanh
        ktd_value = data.get("khoi_tu_doanh", 0)
        try:
            ktd_formatted = (
                f"↑{ktd_value:.0f}" if ktd_value >= 0 else f"↓{abs(ktd_value):.0f}"
            )
            csv_rows.append([f"Khối tự doanh: {ktd_formatted} tỷ đồng"])
        except:
            csv_rows.append(["Khối tự doanh: N/A"])

        # Nhóm ngành nổi bật
        sectors = data.get("top_sectors", [])
        csv_rows.append(
            [f"Nhóm ngành nổi bật: {', '.join(sectors) if sectors else 'N/A'}"]
        )

        # Cổ phiếu tâm điểm
        interested = data.get("top_interested", [])
        csv_rows.append(
            [f"Cổ phiếu tâm điểm: {', '.join(interested) if interested else 'N/A'}"]
        )

        # Tác động tăng/giảm
        impact_up = data.get("impact_up", {})
        impact_down = data.get("impact_down", {})

        if impact_up:
            up_total = impact_up.get("total", 0)
            up_stocks = impact_up.get("stock_code", [])
            csv_rows.append(
                [
                    f"Tác động tăng (+{up_total:.2f}): {', '.join(up_stocks) if up_stocks else 'N/A'}"
                ]
            )

        if impact_down:
            down_total = impact_down.get("total", 0)
            down_stocks = impact_down.get("stock_code", [])
            csv_rows.append(
                [
                    f"Tác động giảm ({down_total:.2f}): {', '.join(down_stocks) if down_stocks else 'N/A'}"
                ]
            )

    def upload_to_s3_with_retry(
        self, local_file_path: str, s3_key: str, max_retries: int = 3
    ) -> bool:
        """Upload to S3 with retry logic"""
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False

        for attempt in range(max_retries):
            try:
                self.s3_client.upload_file(
                    local_file_path, self.s3_config["bucket_name"], s3_key
                )
                logger.info(
                    f"Successfully uploaded to s3://{self.s3_config['bucket_name']}/{s3_key}"
                )
                return True
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                logger.error(f"S3 upload attempt {attempt + 1} failed: {error_code}")

                if attempt == max_retries - 1:
                    return False

                time.sleep(2**attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Unexpected error during S3 upload: {e}")
                if attempt == max_retries - 1:
                    return False

        return False

    def generate_report(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate report with enhanced error handling"""
        try:
            # Validate input data
            validation_result = self.validate_input_data(data)

            if not validation_result["valid"]:
                logger.warning(f"Data validation failed: {validation_result['errors']}")
                # Continue with warnings but log issues

            # Generate CSV content
            csv_rows = self.generate_csv_content_with_metadata(data)
            print(csv_rows)
            # Create temporary file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False, encoding="utf-8-sig"
            ) as temp_file:
                writer = csv.writer(temp_file)
                writer.writerows(csv_rows)
                temp_file_path = temp_file.name

            # Upload to S3
            s3_key = self.s3_config["csv_key"]
            upload_success = self.upload_to_s3_with_retry(temp_file_path, s3_key)

            # Get file size
            file_size = os.path.getsize(temp_file_path)

            # Cleanup
            os.unlink(temp_file_path)

            return {
                "success": upload_success,
                "message": "Enhanced CSV report with percentage columns generated successfully"
                if upload_success
                else "CSV report generation failed",
                "timestamp": datetime.now().isoformat(),
                "s3_location": f"s3://{self.s3_config['bucket_name']}/{s3_key}"
                if upload_success
                else None,
                "file_size_bytes": file_size,
                "data_validation": validation_result,
                "rows_generated": len(csv_rows),
                "enhancements": {
                    "klgd_percentage_column": True,
                    "gtgd_percentage_column": True,
                    "vnxall_excluded": True,
                    "enhanced_formatting": True,
                    "arrow_indicators": True,
                },
            }

        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return {
                "success": False,
                "message": f"CSV report generation failed: {str(e)}",
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
            }


def main(
    data: Dict[str, Any],
    s3_bucket_name: str,
    s3_csv_key: str,
    s3_region: str = "ap-southeast-2",
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
) -> Dict[str, Any]:
    """Enhanced main function with comprehensive error handling"""

    s3_config = {
        "bucket_name": s3_bucket_name,
        "csv_key": s3_csv_key,
        "region": s3_region,
        "access_key_id": aws_access_key_id,
        "secret_access_key": aws_secret_access_key,
    }

    generator = EnhancedStockReportGenerator(s3_config)
    result = generator.generate_report(data)

    # Add configuration info to result
    result["config_used"] = {
        "bucket": s3_bucket_name,
        "key": s3_csv_key,
        "region": s3_region,
        "using_iam": aws_access_key_id is None,
    }

    return result
