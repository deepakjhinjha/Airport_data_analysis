import os
import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "flight-data-deepak"   # change if needed
REGION = "ap-south-1"
LOCAL_FOLDER = "./downloaded_files"

s3_client = boto3.client("s3", region_name=REGION)


# ✅ Create bucket (only for ap-south-1)
def create_bucket_if_not_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket already exists: {bucket_name}")

    except ClientError:
        print(f"Creating bucket: {bucket_name}")

        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )

        print("Bucket created successfully!")


# ✅ Upload with partitioning
def upload_partitioned(file_path):
    file_name = os.path.basename(file_path)

    parts = file_name.split("_")
    year = parts[-2]
    month = int(parts[-1].replace(".csv", ""))

    s3_key = f"flight_data/year={year}/month={month:02d}/{file_name}"

    print(f"Uploading → {s3_key}")

    s3_client.upload_file(file_path, BUCKET_NAME, s3_key)

    print("Upload successful!")


# ✅ Upload all files
def upload_all_files():
    for file in os.listdir(LOCAL_FOLDER):
        if file.endswith(".csv"):
            file_path = os.path.join(LOCAL_FOLDER, file)
            upload_partitioned(file_path)


# 🚀 Run everything
if __name__ == "__main__":
    create_bucket_if_not_exists(BUCKET_NAME)
    upload_all_files()