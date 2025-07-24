import boto3
import io
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
from typing import Dict, Any


def main(
    aws_access_key: str,
    aws_secret_key: str,
    bucket_name: str,
    object_key_name: str,
    date_offset: int = 0,
    region: str = "us-east-1",
) -> Dict[str, Any]:
    """
    Downloads a CSV file from S3 with a dynamically generated key based on the date.

    Args:
        aws_access_key: The AWS access key ID.
        aws_secret_key: The AWS secret access key.
        bucket_name: The name of the S3 bucket.
        object_key_template: A template for the S3 object key with placeholders.
                             Example: "folder/file_{YYYY}{MM}{DD}.csv"
        date_offset: An integer to offset the date. 0 for today, -1 for yesterday.
        region: The AWS region of the bucket.

    Returns:
        A dictionary with the CSV content or an error message.
    """
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    # 1. Determine the target date using the offset (UTC for consistency)
    target_date = datetime.now(timezone.utc) + timedelta(days=date_offset)

    # 2. Create the final S3 object key from the template
    object_key_name += "_{YYYY}{MM}{DD}.csv"
    try:
        final_object_key = object_key_name.format(
            YYYY=target_date.strftime("%Y"),
            MM=target_date.strftime("%m"),
            DD=target_date.strftime("%d"),
        )
        print(f"Attempting to download from S3 key: {final_object_key}")
    except KeyError as e:
        return {
            "success": False,
            "error": f"Invalid placeholder in template: {e}",
            "message": "The object_key_template has an invalid placeholder. Please use {YYYY}, {MM}, or {DD}.",
        }

    # 3. Attempt to download the file
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=final_object_key)
        csv_content = response["Body"].read().decode("utf-8")

        return {
            "success": True,
            "csv_content": csv_content,
            "object_key_used": final_object_key,
            "content_type": response.get("ContentType", "text/csv"),
            "size": len(csv_content),
            "message": f"CSV '{final_object_key}' downloaded successfully.",
        }

    except ClientError as e:
        # Provide a more specific error if the file is not found
        if e.response["Error"]["Code"] == "NoSuchKey":
            error_message = f"File not found at key: '{final_object_key}'. Please check if the file for this date exists in the bucket."
        else:
            error_message = f"An S3 ClientError occurred: {e}"

        return {
            "success": False,
            "error": str(e),
            "message": error_message,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "An unexpected error occurred during the download process.",
        }
