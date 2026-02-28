import json
import boto3
import os
import io
from datetime import datetime
import pandas as pd

def lambda_handler(event, context):
    # Read CSV from S3
    s3 = boto3.client("s3")
    bucket = os.environ["BUCKET_NAME"]
    csv_key = event.get("csv_key", "uploads/countries.csv")

    print(f"Reading CSV from s3://{bucket}/{csv_key}")

    response = s3.get_object(Bucket=bucket, Key=csv_key)
    csv_content = response["Body"].read().decode("utf-8")

    df = pd.read_csv(io.StringIO(csv_content))

    print(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")

    # Add extraction timestamp
    df["extracted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Convert to Parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    date_prefix = datetime.now().strftime("%Y%m%d%H%M%S")

    s3.put_object(
        Bucket=bucket,
        Key=f"csv-data/extracted-{date_prefix}.parquet",
        Body=buffer.getvalue()
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "CSV data saved as Parquet",
            "rows": len(df),
            "columns": list(df.columns),
            "s3_key": f"csv-data/extracted-{date_prefix}.parquet"
        })
    }