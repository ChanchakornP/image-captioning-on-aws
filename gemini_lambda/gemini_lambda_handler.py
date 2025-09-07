# pip install -q -U google-genai pillow PyMySQL
import base64
import json
import os
from io import BytesIO

import boto3
import pymysql
from google import genai
from google.genai import types
from PIL import Image

# Get environment variables
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
secret_name = os.getenv("SECRET_NAME")
region_name = os.getenv("REGION")

client = genai.Client(api_key=GOOGLE_API_KEY)

s3 = boto3.client("s3")


def generate_image_caption(image_data):
    try:
        encoded_image = base64.b64encode(image_data).decode("utf-8")
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[
                types.Part.from_bytes(
                    data=encoded_image,
                    mime_type="image/jpeg",
                ),
                "Caption this image.",
            ],
        )
        return response if isinstance(response, str) else response.text
    except Exception as e:
        return f"Error generating caption: {str(e)}"


def lambda_handler(event, context):
    print("Lambda triggered.")
    print("Event received:", json.dumps(event))

    try:
        sns_message = event["Records"][0]["Sns"]["Message"]
        s3_event = json.loads(sns_message)
        s3_info = s3_event["Records"][0]["s3"]
        bucket = s3_info["bucket"]["name"]
        key = s3_info["object"]["key"]
        print(f"New image uploaded: s3://{bucket}/{key}")
    except Exception as e:
        print(f"Failed to parse SNS/S3 event: {e}")
        return {"statusCode": 400, "body": "Invalid event format"}

    try:
        secrets_client = boto3.client("secretsmanager", region_name=region_name)
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        print(f"Loaded DB secret: host={secret['host']}")
    except Exception as e:
        print(f"Failed to load secret: {e}")
        return {"statusCode": 500, "body": "Secret retrieval failed"}

    try:
        connection = pymysql.connect(
            host=secret["host"],
            user=secret["username"],
            password=secret["password"],
            db=secret["dbname"],
            port=int(secret["port"]),
            connect_timeout=5,
        )
        print("Connected to RDS.")
    except Exception as e:
        print(f"Failed to connect to RDS: {e}")
        return {"statusCode": 500, "body": "Database connection failed"}

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        image_data = response["Body"].read()
        image = Image.open(BytesIO(image_data))
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")

        caption = generate_image_caption(image_data)
        print(f"Generated Caption: {caption}")
    except Exception as e:
        print(f"Error processing image: {e}")
        return {"statusCode": 500, "body": f"Image processing failed: {str(e)}"}

    try:
        filename = os.path.splitext(os.path.basename(key))[0]
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE captions SET caption = %s WHERE image_key = %s",
            (caption, filename),
        )
        connection.commit()
        connection.close()
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Failed to insert data into database: {str(e)}",
        }

    return {"statusCode": 200, "body": f"Caption for {key} saved successfully."}
