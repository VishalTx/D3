from utils.aws_wrapper import AWSApi

def test_check_aws_api_update_success():
    aws_obj = AWSApi()
    status_code, response = aws_obj.generate()
    assert status_code == 200