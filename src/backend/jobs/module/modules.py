import boto3
from datetime import datetime
import re

def get_latest_partition_datetime(bucket, prefix):
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio_access_key",
        aws_secret_access_key="minio_secret_key"
    )

    #Create paginator (phân trang) for list operator (in this case is list_objects) to accquire full returned value of objects
    paginator = s3_client.get_paginator("list_objects_v2")
    # Iterator to choose which page has Bucket=bucket, Prefix=prefix (in the list type)
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    timestamps = []
    # Hỗ trợ cả path có 6 phần (full datetime) và 4 phần (date + hour)
    pattern_full = re.compile(r"(\d{4})/(\d{2})/(\d{2})/(\d{2})/(\d{2})/(\d{2})")
    pattern_hour = re.compile(r"(\d{4})/(\d{2})/(\d{2})/(\d{2})")

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            match = pattern_full.search(key)
            if match:
                year, month, day, hour, minute, second = map(int, match.groups())
                timestamps.append(datetime(year, month, day, hour, minute, second))
                continue

            match = pattern_hour.search(key)
            if match:
                year, month, day, hour = map(int, match.groups())
                timestamps.append(datetime(year, month, day, hour))

    if not timestamps:
        print("No valid timestamp found in keys")
        return None

    latest = max(timestamps)
    print(f"✅ Latest timestamp found: {latest}")
    return latest


# #version 2
# def get_latest_partition_date(bucket, prefix):
#     from datetime import datetime
#     import boto3

#     s3_client = boto3.client(
#         "s3",
#         endpoint_url="http://minio:9000",
#         aws_access_key_id="minio_access_key",
#         aws_secret_access_key="minio_secret_key"
#     )

#     response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

#     if 'Contents' not in response:
#         print(f"There is no data available in {prefix}, ready to load")
#         return None

#     contents = response.get("Contents", [])
#     partitions = set()

#     for obj in contents:
#         key = obj["Key"]
#         parts = key.split("/")  # tách đường dẫn thành list

#         try:
#             year = int(parts[-7])
#             month = int(parts[-6])
#             day = int(parts[-5])
#             hour = int(parts[-4])
#             minute = int(parts[-3])
#             second = int(parts[-2])
#             partitions.add(datetime(year, month, day, hour, minute, second))
#         except:
#             continue

#     if not partitions:
#         return None

#     latest = max(partitions)
#     print(f"✅ Latest partition timestamp: {latest}")
#     return latest