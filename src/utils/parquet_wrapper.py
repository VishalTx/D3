from utils.aws_wrapper import AWSS3
import os
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

class ParquetWrapper(AWSS3):
    def __init__(self):
        super().__init__()

        # setting/validating local folders
        self.parquet_folder = self.getenv("Parquet_Path")
        self.csv_folder = self.getenv("Parquet_Path")

        if not os.path.isdir(self.parquet_folder):
            os.makedirs(self.parquet_folder, exist_ok=True)
        if not os.path.isdir(self.csv_folder):
            os.makedirs(self.csv_folder, exist_ok=True)

    def read_parquet(self, file_name):
        try:
            table = pq.read_table(file_name)
            return table.to_pandas()
        except:
            return False

    def write_to_csv(self, df, csv_file_name):
        df.to_csv(csv_file_name)

    def get_aws_s3_object_data(self, bucket_name, bucket_key):
        # Setup credentials from properties
        environment = self.getenv("Environment")
        os.environ["AWS_ACCESS_KEY_ID"] = self.getenv(f"{environment}_AWS_ACCESS_KEY_ID")
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.getenv(f"{environment}_AWS_SECRET_ACCESS_KEY")
        os.environ["AWS_SESSION_TOKEN"] = self.getenv(f"{environment}_AWS_SESSION_TOKEN")

        parquet_path = os.path.join(self.parquet_folder, "S3Source.parquet")
        csv_path = os.path.join(self.csv_folder, "S3Source.csv")

        parquet_path = Path(parquet_path)
        csv_path = Path(csv_path)

        parquet_path.unlink(missing_ok=True)
        csv_path.unlink(missing_ok=True)

        # Getting data from S3 bucket
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=bucket_key)
            with open(str(parquet_path), 'wb') as f:
                f.write(response['Body'].read())
            print("Successfully obtained bytes from an S3 object")
        except Exception as e:
            print(f"Failed to download object: {e}")
            return False

        try:
            table = pq.read_table(parquet_path)
            df = table.to_pandas()
            print(df.head(10))
            df.to_csv(csv_path, index=False)
            print("Table CSV written successfully.")
        except Exception as e:
            print(f"Failed to process Parquet file: {e}")
            return False

        return True

if __name__ == "__main__":
    pw = ParquetWrapper()
    bucket_name = "dlx-ddm-process-dev"
    key_name = "consumer/speedeon/premover_contract/movedistance/dt=20220406/part-00006-eb929362-53cb-4c2e-866c-bd01f402e28d.c000.snappy.parquet"
    pw.get_aws_s3_object_data(bucket_name, key_name)
