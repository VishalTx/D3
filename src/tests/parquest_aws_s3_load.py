from utils.parquet_wrapper import ParquetWrapper

def test_get_aws_parquet_s3_object_data():
    pw = ParquetWrapper()
    bucket_name = "dlx-ddm-process-dev"
    key_name = "consumer/speedeon/premover_contract/movedistance/dt=20220406/part-00006-eb929362-53cb-4c2e-866c-bd01f402e28d.c000.snappy.parquet"
    response = pw.get_aws_s3_object_data(bucket_name, key_name)
    assert response == True
