import re
from enum import Enum
from datetime import datetime


class StockMarket(Enum):
    HSX = "HSX"
    HNX = "HNX"
    UPCOM = "UPCOM"


def validate_stock_market(market: str) -> str:
    if not market:
        return "HSX"
    try:
        return StockMarket(market.upper()).value
    except ValueError:
        print(f"Invalid stock market: {market}, defaulting to HSX")
        return "HSX"


def generate_datetime_csv_key(base_name: str = "stock_summary_report") -> str:
    """Generate CSV key with datetime stamp"""
    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    # time_str = now.strftime("%H%M%S")
    return f"{base_name}_{date_str}.csv"


def generate_datetime_excel_key(base_name: str = "stock_summary_report") -> str:
    """Generate Excel key with datetime stamp"""
    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    # time_str = now.strftime("%H%M%S")
    return f"{base_name}_{date_str}.xlsx"


def validate_s3_config(bucket: str, region: str) -> dict:
    errors = []
    if not bucket or len(bucket) < 3:
        errors.append("S3 bucket name is required and must be at least 3 characters")
    if not region:
        errors.append("S3 region is required")

    # Validate bucket name format
    if bucket and not re.match(r"^[a-z0-9.-]{3,63}$", bucket):
        errors.append("Invalid S3 bucket name format")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "validated_config": {"bucket": bucket, "region": region},
    }


def main(
    stock_market: str,
    s3_bucket_name: str,
    s3_region: str,
    base_name: str = "stock_summary_report",
):
    validated_market = validate_stock_market(stock_market)

    # Generate datetime-based keys
    csv_key = generate_datetime_csv_key(base_name)
    excel_key = generate_datetime_excel_key(base_name)

    s3_validation = validate_s3_config(s3_bucket_name, s3_region)

    if not s3_validation["valid"]:
        raise ValueError(f"Configuration errors: {', '.join(s3_validation['errors'])}")

    return {
        "stock_market": validated_market,
        "s3_config": {
            **s3_validation["validated_config"],
            "csv_key": csv_key,
            "excel_key": excel_key,
        },
        "validation_passed": True,
        "generated_keys": {
            "csv": csv_key,
            "excel": excel_key,
            "timestamp": datetime.now().isoformat(),
        },
    }
