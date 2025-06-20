from utils.aws_wrapper import AWSS3

def test_aws_s3_load():
    aws_obj = AWSS3()
    # provide known bucket name to test i.e. `adstra_americas`
    bucket_items = aws_obj.load_s3_bucket("adstra_americas")
    assert bucket_items is not None

