import boto3
import csv
import io
from botocore.exceptions import ClientError


def main(
    aws_access_key: str,
    aws_secret_key: str,
    bucket_name: str,
    object_key: str,
    region: str = "us-east-1",
):
    """Download CSV from S3 and return content"""

    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    try:
        # Download object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Read the CSV data
        csv_content = response["Body"].read().decode("utf-8")

        return {
            "success": True,
            "csv_content": csv_content,
            "content_type": response.get("ContentType", "text/csv"),
            "size": len(csv_content),
            "message": "CSV downloaded successfully",
        }

    except ClientError as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to download CSV from S3",
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Unexpected error occurred",
        }
