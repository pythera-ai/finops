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


def main(s3_config: dict, aws_access_key_id: str, aws_secret_access_key: str):
    """
    Downloads yesterday's CSV report from S3 and parses it to extract
    the 'GTGD (tỷ)' and 'KLGD (triệu cp)' for each index.
    """
    s3_client = boto3.client(
        "s3",
        region_name=s3_config.get("region"),
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    yesterday = datetime.now() - timedelta(days=1)
    base_name_parts = s3_config.get("csv_key", "").split("_")
    if not base_name_parts:
        return {
            "success": False,
            "last_day_gtdg": {},
            "last_day_klgd": {},
            "error": "Invalid base_name structure in s3_config.",
        }
    base_name = "_".join(base_name_parts[:-1])
    yesterday_key = f"{base_name}_{yesterday.strftime('%Y%m%d')}.csv"

    print(f"Attempting to download last day's report from: {yesterday_key}")

    try:
        response = s3_client.get_object(Bucket=s3_config["bucket"], Key=yesterday_key)
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
                            "error": "KLGD (triệu cp) column not found in last day's CSV.",
                        }

                except ValueError:
                    return {
                        "success": False,
                        "last_day_gtdg": {},
                        "last_day_klgd": {},
                        "error": "Required columns not found in last day's CSV.",
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
        }

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {
                "success": True,
                "last_day_gtdg": {},
                "last_day_klgd": {},
                "message": f"Last day CSV not found at {yesterday_key}.",
            }
        return {
            "success": False,
            "last_day_gtdg": {},
            "last_day_klgd": {},
            "error": str(e),
        }
    except Exception as e:
        return {
            "success": False,
            "last_day_gtdg": {},
            "last_day_klgd": {},
            "error": f"Unexpected error processing last day's CSV: {e}",
        }


# Example usage function to demonstrate the enhanced functionality
def process_stock_data_example():
    """
    Example function showing how to use the enhanced data extraction
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
        print("GTGD Data (tỷ đồng):")
        for index, value in result["last_day_gtdg"].items():
            print(f"  {index}: {value}")

        print("\nKLGD Data (triệu cổ phiếu):")
        for index, value in result["last_day_klgd"].items():
            print(f"  {index}: {value}")
    else:
        print(f"Error: {result.get('error', 'Unknown error')}")

    return result
