import time
from botocore.exceptions import ClientError
from utils.aws_wrapper import AWS, AWSCloudwatch
from utils.jira_wrapper import log_defect

class AWSGlueWrapper(AWS):
    def __init__(self):
        super().__init__()
        self.client = self.session.client(
            'glue',
            region_name=self.region
        )
        self.cloudwatch = AWSCloudwatch()

    def monitor(self, job_name, job_run_id, poll_interval=10):
        state = None
        try:
            while True:
                response = self.client.get_job_run(
                    JobName=job_name,
                    RunId=job_run_id
                )
                state = response['JobRun']['JobRunState']
                print("Current State: ", state)
                if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                    break

                time.sleep(poll_interval)
        except ClientError as e:
            print(f"Error fetching job run status: {e.response['Error']['Message']}")
            return False

        if state == 'SUCCEEDED':
            print("Glue job completed successfully.")
            return True
        else:
            print(f"Glue job did not complete successfully. Final status: {state}")
            print(f"Pull Cloudwatch logs and Create Jira ticket")
            log_group = '/aws-glue/jobs/logs-v2'
            log_stream_name = job_run_id
            logs = self.cloudwatch.pull_logs_stream(log_group, log_stream_name)
            logs_message = '\n'.join(logs)
            log_defect(f"AUTOMATION UTILITY: Glue job failed", f"Final status: {state}\n\n{logs_message}")
            return False

    def run_job(self, job_name, job_arguments):
        try:
            response = self.client.start_job_run(
                JobName=job_name,
                Arguments=job_arguments
            )
            job_run_id = response['JobRunId']
            print(f"Glue Job Started. Job Run ID: {job_run_id}")
            self.monitor(job_name, job_run_id)
            return job_run_id
        except ClientError as e:
            print(f"Failed to start Glue job: {e.response['Error']['Message']}")
            return None


if __name__ == "__main__":
    glue = AWSGlueWrapper()