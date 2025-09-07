import json
import os
from io import BytesIO

import boto3
from PIL import Image

s3 = boto3.client("s3")


def lambda_handler(event, context):
    print("Lambda triggered.")
    print("Event received:", json.dumps(event))

    try:
        # Extract and parse the SNS message
        sns_message = event["Records"][0]["Sns"]["Message"]
        print("SNS Message:", sns_message)

        s3_event = json.loads(sns_message)
        s3_info = s3_event["Records"][0]["s3"]

        bucket = s3_info["bucket"]["name"]
        key = s3_info["object"]["key"]
        print(f"New image uploaded: s3://{bucket}/{key}")

        if key.startswith("thumbnail/"):
            print("This is a thumbnail image. Skipping processing to prevent loop.")
            return {"statusCode": 200, "body": "Thumbnail detected. Skipping."}

    except Exception as e:
        print(f"Failed to parse SNS/S3 event: {e}")
        return {"statusCode": 400, "body": "Invalid event format"}

    try:
        # Get image from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        image_data = response["Body"].read()
        image = Image.open(BytesIO(image_data))
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")

        # Create thumbnail
        thumbnail_size = (128, 128)
        image.thumbnail(thumbnail_size)

        # Prepare thumbnail for upload
        buffer = BytesIO()
        image.save(buffer, format="JPEG")
        buffer.seek(0)

        # Generate thumbnail key
        base_name = os.path.basename(key)
        thumb_key = f"thumbnail/{os.path.splitext(base_name)[0]}.jpg"

        # Upload thumbnail to S3
        s3.put_object(
            Bucket=bucket, Key=thumb_key, Body=buffer, ContentType="image/jpeg"
        )

        print(f"Thumbnail saved to s3://{bucket}/{thumb_key}")
        return {
            "statusCode": 200,
            "body": json.dumps(f"Thumbnail saved to {thumb_key}"),
        }

    except Exception as e:
        print(f"Error processing image: {e}")
        return {"statusCode": 500, "body": f"Failed to generate thumbnail: {str(e)}"}
