import os
from os import getenv
from dotenv import load_dotenv
from utils.aws_wrapper import AWSApi

def test_check_aws_api_update_success():
    load_dotenv()
    prefix = os.getenv("Environment").upper().strip()
    aws_obj = AWSApi()
    response, creds = aws_obj.generate()

    assert response == 200
    assert creds["accessKeyId"] == os.getenv(f"{prefix}_AWS_ACCESS_KEY_ID")
    assert creds["secretAccessKey"] == os.getenv(f"{prefix}_AWS_SECRET_ACCESS_KEY")
    assert creds["sessionToken"] == os.getenv(f"{prefix}_AWS_SESSION_TOKEN")
