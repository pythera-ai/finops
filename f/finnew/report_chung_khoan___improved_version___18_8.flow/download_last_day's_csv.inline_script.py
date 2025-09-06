import boto3
import csv
import io
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

MAPPING_KEY = {
    "VN30": "VN30",
    "HNX30": "HNX30",
    "UPCOM": "HNXUpcomIndex",
    "VNXALL": "VNXALL",
    "VNINDEX": "VNINDEX",
    "HNXINDEX": "HNXIndex",
}


def get_last_trading_day():
    """
    Tính toán ngày giao dịch gần nhất:
    - Nếu hôm nay là Thứ 2 (weekday = 0), lấy dữ liệu Thứ 6 tuần trước
    - Các ngày khác thì lấy dữ liệu ngày hôm trước
    """
    today = datetime.now()

    if today.weekday() == 0:  # Monday
        # Lấy dữ liệu Thứ 6 tuần trước (3 ngày trước)
        last_trading_day = today - timedelta(days=3)
        print(
            f"Hôm nay là Thứ 2, sẽ lấy dữ liệu Thứ 6: {last_trading_day.strftime('%Y-%m-%d')}"
        )
    else:
        # Lấy dữ liệu ngày hôm trước
        last_trading_day = today - timedelta(days=1)
        print(f"Lấy dữ liệu ngày trước: {last_trading_day.strftime('%Y-%m-%d')}")

    return last_trading_day


def main(s3_config: dict, aws_access_key_id: str, aws_secret_access_key: str):
    """
    Downloads last trading day's CSV report from S3 and parses it to extract
    the 'GTGD (tỷ)' and 'KLGD (triệu cp)' for each index.
    Improved to handle Monday runs by fetching Friday's data.
    """
    s3_client = boto3.client(
        "s3",
        region_name=s3_config.get("region"),
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # Sử dụng logic mới để tính ngày giao dịch gần nhất
    last_trading_day = get_last_trading_day()

    base_name_parts = s3_config.get("csv_key", "").split("_")
    if not base_name_parts:
        return {
            "success": False,
            "last_day_gtdg": {},
            "last_day_klgd": {},
            "error": "Invalid base_name structure in s3_config.",
        }

    base_name = "_".join(base_name_parts[:-1])
    target_key = f"{base_name}_{last_trading_day.strftime('%Y%m%d')}.csv"

    print(f"Attempting to download last trading day's report from: {target_key}")

    try:
        response = s3_client.get_object(Bucket=s3_config["bucket"], Key=target_key)
        csv_content = response["Body"].read().decode("utf-8-sig")
        csv_file = io.StringIO(csv_content)
        reader = csv.reader(csv_file)

        headers = []
        last_day_gtdg = {}
        last_day_klgd = {}

        for row in reader:
            if not row:
                continue

            # Look for headers row containing both required columns
            if "CHỈ SỐ" in row and "GTGD (tỷ)" in row and "KLGD (triệu cp)" in row:
                headers = row
                try:
                    index_col = headers.index("CHỈ SỐ")
                    gtdg_col = headers.index("GTGD (tỷ)")
                    # Handle different possible variations of the KLGD column name
                    klgd_col = None
                    for i, header in enumerate(headers):
                        if "KLGD" in header and (
                            "triệu cp" in header or "triệu" in header
                        ):
                            klgd_col = i
                            break

                    if klgd_col is None:
                        return {
                            "success": False,
                            "last_day_gtdg": {},
                            "last_day_klgd": {},
                            "error": "KLGD (triệu cp) column not found in trading day's CSV.",
                            "trading_date": last_trading_day.strftime("%Y-%m-%d"),
                        }

                except ValueError:
                    return {
                        "success": False,
                        "last_day_gtdg": {},
                        "last_day_klgd": {},
                        "error": "Required columns not found in trading day's CSV.",
                        "trading_date": last_trading_day.strftime("%Y-%m-%d"),
                    }
                continue

            # Process data rows
            if headers:
                try:
                    if len(row) > max(index_col, gtdg_col, klgd_col):
                        index_name = row[index_col].strip()

                        # Check if this index is in our mapping
                        if index_name in MAPPING_KEY:
                            mapped_index_name = MAPPING_KEY[index_name]

                            # Extract GTGD value (handle colon separation if present)
                            gtdg_value_str = row[gtdg_col].split(":")[0].strip()
                            # Remove any non-numeric characters except decimal point and minus
                            gtdg_clean = "".join(
                                c for c in gtdg_value_str if c.isdigit() or c in ".-"
                            )
                            if gtdg_clean:
                                last_day_gtdg[mapped_index_name] = float(gtdg_clean)

                            # Extract KLGD value
                            klgd_value_str = row[klgd_col].split(":")[0].strip()
                            # Remove any non-numeric characters except decimal point and minus
                            klgd_clean = "".join(
                                c for c in klgd_value_str if c.isdigit() or c in ".-"
                            )
                            if klgd_clean:
                                last_day_klgd[mapped_index_name] = float(klgd_clean)

                except (IndexError, ValueError, AttributeError) as e:
                    # Log the error but continue processing other rows
                    print(f"Error processing row {row}: {e}")
                    continue

        return {
            "success": True,
            "last_day_gtdg": last_day_gtdg,
            "last_day_klgd": last_day_klgd,
            "trading_date": last_trading_day.strftime("%Y-%m-%d"),
        }

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {
                "success": True,
                "last_day_gtdg": {},
                "last_day_klgd": {},
                "message": f"Trading day CSV not found at {target_key}.",
                "trading_date": last_trading_day.strftime("%Y-%m-%d"),
            }
        return {
            "success": False,
            "last_day_gtdg": {},
            "last_day_klgd": {},
            "error": str(e),
            "trading_date": last_trading_day.strftime("%Y-%m-%d"),
        }
    except Exception as e:
        return {
            "success": False,
            "last_day_gtdg": {},
            "last_day_klgd": {},
            "error": f"Unexpected error processing trading day's CSV: {e}",
            "trading_date": last_trading_day.strftime("%Y-%m-%d"),
        }


def get_previous_trading_days(num_days=5):
    """
    Utility function để lấy nhiều ngày giao dịch trước đó
    Hữu ích cho việc phân tích hoặc fallback
    """
    today = datetime.now()
    trading_days = []

    current_date = today
    while len(trading_days) < num_days:
        # Lùi 1 ngày
        current_date = current_date - timedelta(days=1)

        # Bỏ qua Chủ Nhật (6) và Thứ 7 (5)
        if current_date.weekday() not in [5, 6]:
            trading_days.append(current_date)

    return trading_days


# Enhanced example usage function
def process_stock_data_example():
    """
    Example function showing how to use the enhanced data extraction
    with proper trading day logic
    """
    # Example configuration
    s3_config = {
        "bucket": "your-bucket-name",
        "csv_key": "stock_reports/stock_summary_report_20250721.csv",
        "region": "us-east-1",
    }

    # Call the main function
    result = main(s3_config, "your_access_key", "your_secret_key")

    if result["success"]:
        print(f"Dữ liệu từ ngày giao dịch: {result.get('trading_date', 'N/A')}")
        print("\nGTGD Data (tỷ đồng):")
        for index, value in result["last_day_gtdg"].items():
            print(f"  {index}: {value}")

        print("\nKLGD Data (triệu cổ phiếu):")
        for index, value in result["last_day_klgd"].items():
            print(f"  {index}: {value}")

        if not result["last_day_gtdg"] and not result["last_day_klgd"]:
            print("Không tìm thấy dữ liệu hoặc file không tồn tại.")
    else:
        print(f"Error: {result.get('error', 'Unknown error')}")
        print(f"Ngày giao dịch được tìm kiếm: {result.get('trading_date', 'N/A')}")

    return result


# Debug function to test different days
def test_trading_day_logic():
    """
    Function để test logic trading day cho các ngày khác nhau
    """
    print("Testing trading day logic:")
    print("=" * 50)

    # Test với các ngày trong tuần
    test_dates = [
        datetime(2024, 1, 1),  # Monday
        datetime(2024, 1, 2),  # Tuesday
        datetime(2024, 1, 3),  # Wednesday
        datetime(2024, 1, 4),  # Thursday
        datetime(2024, 1, 5),  # Friday
        datetime(2024, 1, 6),  # Saturday
        datetime(2024, 1, 7),  # Sunday
    ]

    for test_date in test_dates:
        # Temporarily override datetime.now() for testing
        original_now = datetime.now
        datetime.now = lambda: test_date

        try:
            trading_day = get_last_trading_day()
            weekday_name = test_date.strftime("%A")
            print(
                f"{weekday_name} ({test_date.strftime('%Y-%m-%d')}) -> Trading day: {trading_day.strftime('%Y-%m-%d')}"
            )
        finally:
            # Restore original datetime.now
            datetime.now = original_now
