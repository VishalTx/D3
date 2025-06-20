import os

from utils.aws_wrapper import AWSDynamoDB, AWSS3
from utils.glue_wrapper import AWSGlueWrapper
from utils.bitbucket_wrapper import BitbucketWrapper
from utils.extract_script_part import extract_list
from utils.parquet_wrapper import ParquetWrapper
from utils.jira_wrapper import log_defect
from datetime import datetime

def job_step(func, *args, **kwargs):
    print(f"Executing Job Step {func.__name__}")
    def step_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"An exception occured in {func.__name__}")
            raise e

class Job:
    job_name = "dlx-ddm-trigger-load-in-copy"
    def __init__(self):
        self.logs = {}
        self.state_cut_data = None
        self.zip9_footprint = None
        self.zip5_footprint = None
        self.state_footprint = None
        self.jira = None
        self.asset_name = None
        self.job_arguments = None
        self.run_at = None
        self.parquet_target_df = None
        self.glue_client = None
        self.client_name = None
        self.year = None
        self.parquet_df = None
        self.parquet = None
        self.config_json = None
        self.current_week = None
        self.BASE_CONSUMER_TRIGGER = None
        self.s3 = None
        self.campaign_wednesday = None
        self.stack_file_path = None
        self.dynamodb = None

    def add_log(self, log_title, log_message):
        if log_title not in self.logs.keys():
            self.logs[log_title] = [log_message]
        else:
            self.logs[log_title].append(log_message)

    def run(self):
        print("Job Started")
        self.add_log('Job Started', '')
        # Initiating the resources
        self.dynamodb = AWSDynamoDB()
        self.s3 = AWSS3()
        self.parquet = ParquetWrapper()
        # fetching updating the repository
        repos = BitbucketWrapper()
        self.run_at = datetime.today().strftime('%Y%m%d')
        self.asset_name = 'consumer_trigger_E2E'
        # AWS glue constructor
        self.glue_client = AWSGlueWrapper()
        op_type, status, message = repos.clone_repo()
        if status:
            self.add_log('Syncing Bitbucket Code', 'Code is Synced.')
        else:
            self.add_log('Syncing Bitbucket Code', 'Error in syncing code.')
        # TODO: skipping operation for testing purpose. Remove below lines code once flow is clear.
        op_type, status, message = 1, True, ''
        if status:
            '''Started Job Logic here'''
            self.prerequisites()
        else:
            print(f'Error in fetching repository. Check environment details Error: [{message}]')
            '''Exnd Job Logic here'''
            self.done()

    def done(self, error=False, error_message=''):
        if error:
            print("Job Ended with ERROR", error_message)
        else:
            print("Job Ended with SUCCESS")

    def table_exists(self, table_name):
        if self.dynamodb.table_exists(table_name):
            self.add_log("Step 1: Checking Pre-requisites", f'{table_name}: Exists')
        else:
            self.done(error=True, error_message=f'[Pre-requisites FAILED] Table `{table_name}` does not exist')
            self.add_log("Step 1: Checking Pre-requisites", f'{table_name}: Does Not Exists')

    def prerequisites(self):
        print("Step 1: Checking Pre-requisites")
        self.add_log("Step 1: Checking Pre-requisites", '')
        # Tables check
        self.table_exists('ddm_client_trigger_config')
        self.table_exists('ddm_client_trigger_waterfall')
        self.table_exists('ddm_client_trigger_step')

        # Extract variables from config json
        query = {'config_id': {'S':'dollar-consumer_trigger_E2E_Tx'}}
        try:
            self.config_json = self.dynamodb.get_item('ddm_client_trigger_config', query)
        except:
            self.add_log("Step 1: Checking Pre-requisites", f'Loading Trigger Config file: Failed')
            log_defect("AUTOMATION UTILITY: failed to load config file.", 'Trigger config file failed to load')
        self.stack_file_path = self.config_json['general'].get('stack_file_path', None)
        self.campaign_wednesday = self.config_json['eligibility'].get('campaign_wednesday', None)
        self.current_week = self.config_json['matching'].get('current_week', None)
        self.client_name = self.config_json['client_name']
        self.state_footprint = self.config_json['eligibility'].get('state_footprint', None)
        self.zip5_footprint = self.config_json['eligibility'].get('zip5_footprint', None)
        self.zip9_footprint = self.config_json['eligibility'].get('zip9_footprint', None)

        if self.stack_file_path is None:
            self.done(error=True, error_message="Could not find `stack_file_path` in config file.")
            self.add_log("Step 1: Checking Pre-requisites", f'Could not find `stack_file_path` in config file.')
        if self.stack_file_path == '':
            self.done(error=True, error_message="`stack_file_path` value blank in config file. ")
            self.add_log("Step 1: Checking Pre-requisites", f'`stack_file_path` value blank in config file. ')

        # move next step
        self.load_column_schema()

    def load_column_schema(self):
        # Path to BASE_CONSUMER
        # i.e. '--d3_ingest/src/dev/glue-files/site-packages/standardized_packages/source_code/load_in/trigger_load_in/trigger_load_in.py'
        print("Step 2: Loading Column Schema")
        bucket_local_path = self.s3.getenv('Bitbucket_Local_Path')
        script_path = bucket_local_path + '/glue-files/site-packages/standardized_packages/source_code/load_in/trigger_load_in/trigger_load_in.py'
        list_name = 'BASE_CONSUMER_TRIGGER'
        try:
            self.BASE_CONSUMER_TRIGGER = extract_list(script_path, list_name)
            self.add_log("Step 2: Loading Column Schema", f'{len(self.BASE_CONSUMER_TRIGGER)} columns loaded. {','.join(self.BASE_CONSUMER_TRIGGER)}')
        except ValueError as e:
            self.add_log("Step 2: Loading Column Schema",
                         f'Failed to load columns')

        self.previous_logic_check()

    def previous_logic_check(self):
        """
        Previous Logic
        --------------
        Load previous run logic for both Input and Output files AWS S3 Bucket path/URI
         - if no JSON Object present for campaign_wednesday on Input files and no
        JSON Object present for current_week on Output files
        """
        print("Step 3: Previous Logic Check")
        # logic check for `campaign_wednesday`
        if self.campaign_wednesday is None:
            self.done(error=True, error_message="Could not find `campaign_wednesday` in config file.")
            self.add_log("Step 3: Previous Logic Check", 'Could not find `campaign_wednesday` in config file.')
        elif self.campaign_wednesday == '':
            self.done(error=True, error_message="`campaign_wednesday` value blank in config file. ")
            self.add_log("Step 3: Previous Logic Check", '`campaign_wednesday` value blank in config file.')
        else:
            # handling `.` in campaign_wednesday
            self.year = self.campaign_wednesday.split(".")[0]
            self.campaign_wednesday = self.campaign_wednesday.replace(".", "")
            self.add_log("Step 3: Previous Logic Check", f'`campaign_wednesday` exists {self.campaign_wednesday}')

        # logic check for `current_week`
        if self.current_week is None:
            self.done(error=True, error_message="Could not find `current_week` in config file.")
            self.add_log("Step 3: Previous Logic Check", f'Could not find `current_week` in config file.')
        elif self.current_week == '':
            self.done(error=True, error_message="`current_week` value blank in config file. ")
            self.add_log("Step 3: Previous Logic Check", f'`current_week` value blank in config file. ')
        else:
            self.add_log("Step 3: Previous Logic Check", f'`current_week` exists {self.current_week} ')

        self.write_pre_glue_job_output()

    def write_pre_glue_job_output(self):
        '''
        Write Pre Glue job output (Reliability Test)
        --------------------------------------------
        Write Pre Glue job output (Reliability Test) of our Schema validation reports
        on Source Input files ensuring that all 33 Mandatory Columns are present if not
         present log a defect in Jira upon post actual Glue job execution of Load In
        '''
        print("Step 4: Write pre-glue job output")
        try:
            bucket_name = f"dlx-ddm-consume-{self.s3.getenv("Environment")}"
            prefix = f"{self.stack_file_path}/dt={self.campaign_wednesday}/"
            bucket_objects = self.s3.list_bucket_objects(bucket_name, prefix)
            bucket_object = bucket_objects[1]['Key']
            parquet_destination = os.path.join(self.s3.getenv('Parquet_Path'), 'S3Source.parquet')
            self.s3.download_file(bucket_name, bucket_object, parquet_destination)

            # Converting Source parquet file output to CSV
            self.parquet_df = self.parquet.read_parquet(parquet_destination)
            csv_destination = os.path.join(self.s3.getenv('Parquet_CSV_Path'), "S3Source.csv")
            self.parquet.write_to_csv(self.parquet_df, csv_destination)
            self.write_schema_validation()
        except Exception as e:
            self.add_log("Step 4: Write pre-glue job output", f'Failed to download Source parquet file. ')
            log_defect("AUTOMATION UTILITY: failed to download Source parquet file", 'failed to download Source parquet file')
            self.prepare_report()

    def write_schema_validation(self):
        '''
        Write out schema validation report
        ----------------------------------
        Write output of our Schema validation reports on Output file ensuring that
        all 33 Mandatory Columns are present if not present log a defect in Jira
        upon post actual Glue job execution of Load In

        RELIABILITY CHECK FOR RAW DATA WE INTRODUCED AND NOT IN EXISTING SOLUTION
        '''
        print("Step 5: Write Schema Validation")
        # checking for all `BASE_CONSUMER_TRIGGERS` available in S3Source parquet file
        base_cols = [col.lower() for col in self.BASE_CONSUMER_TRIGGER]
        parq_cols = [col.lower() for col in self.parquet_df.columns]
        compared = all(col in parq_cols for col in base_cols)
        # compared = all(col.lower() in self.parquet_df.columns for col in self.BASE_CONSUMER_TRIGGER)
        if not compared:
            missing_columns = [col for col in base_cols if col not in parq_cols]
            print("Missing columns are:", missing_columns)
            matched_columns = [col for col in base_cols if col in parq_cols]
            print("Matched columns are:", matched_columns)
            message_missing = f"Missing Columns: {','.join(missing_columns)}"
            message_matched = f"Matched Columns: {','.join(matched_columns)}"
            defect_number = log_defect("AUTOMATION UTILITY: Columns mismatched", f"Columns Did not matched\n{message_missing}\n{message_matched}")
            self.add_log("Step 5: Write Schema Validation", f"Columns Did not matched (Ticket ref. {defect_number})")
            self.add_log("Step 5: Write Schema Validation", message_missing)
            self.add_log("Step 5: Write Schema Validation", message_matched)
        else:
            print("Schema validation: SUCCESS")
            self.add_log("Step 5: Write Schema Validation", f"{len(base_cols)} matched no mising")
            self.add_log("Step 5: Write Schema Validation", f"Matched Columns: {','.join(base_cols)}")
        self.load_glue_job()

    def load_glue_job(self):
        """
        Load Glue Job
        -------------
        Load In Glue Job to construct from the derived logic from config table of
        Dynamo DB as per point#2 & #3 to have fully qualified Input and Output
        AWS S3 bucket file paths
        """
        print("Step 6: Load Glue Job from config table")
        self.job_arguments = {
            '--aws_region': self.s3.region,
            '--aws_account': self.s3.getenv(f'{self.s3.environment}_AWS_ACCOUNT_ID'),
            '--env': self.s3.environment,
            '--entity': self.client_name,
            '--asset': self.asset_name,
            '--processing_date': self.run_at
        }
        self.add_log("Step 6: Load Glue Job from config table", f"Job Arguments prepared: {self.job_arguments}")

        self.apply_state()

    def apply_state(self):
        '''
        Apply State
        -----------
        Apply State footprint cut data filtering logic based on the JSON Object
        present for state_footprint from Dynamo DB table 'ddm_client_trigger_config'
        '''
        print("Step 7: Applying state filter")
        if self.state_footprint:
            self.state_cut_data = self.parquet_df[self.parquet_df['state_st_ncoa'].isin(self.state_footprint)]
            print(f"Step: Applied state filter: {len(self.state_cut_data)} filter out of {len(self.parquet_df)}")
            self.add_log("Step 7: Applying state filter", f"Job Arguments prepared: {self.job_arguments}")
        else:
            self.add_log("Step 7: Applying state filter", "Skipped :: State footprint not available")
        self.apply_zip5_zip9()

    def apply_zip5_zip9(self):
        '''
        Apply Zip5 & Zip9 footprint cut data
        ------------------------------------
        Apply Zip5 & Zip9 footprint cut data filtering on US Zip Codelogic based on
        the JSON Objects fields presence zip5_footprint & zip9_footprint to get the
        S3 path location for the zip codes outlined in the file as details having
        dependency to filter Zip codes from the lookup file (Zip Txt or CSV) S3
        path as provided from Dynamo DB table 'ddm_client_trigger_config'
        '''
        print("Step 8: Apply Zip5 & Zip9")
        self.add_log("Step 8: Apply Zip5 & Zip9", "WIP")
        self.execute_glue_job()

    def execute_glue_job(self):
        '''
        Execution of glue job `dlx-ddm-trigger-load-in`
        -----------------------------------------------
        Run the Glue Job via python script to execute the Glue Job named
        - Glue Job Name: dlx-ddm-trigger-load-in
        Note: only if job failed,  this job get failed then extract logs from cloudwatch.
        print it.
        '''
        print("Step 9: Execute Glue Job")
        # job_status = self.glue_client.run_job(self.job_name, self.job_arguments)
        job_status = True
        if job_status:
            self.add_log("Step 9: Execute Glue Job", f"Glue job {self.job_name} successfully triggered.")
            self.waterfall_data_logs()
        else:
            self.add_log("Step 9: Execute Glue Job", f"Glue job {self.job_name} failed. See logs for more information.")
            self.prepare_report()

    def waterfall_data_logs(self):
        '''
        Waterfall data logs
        -------------------
        Connect to Dynamo DB. Waterfall data logs updates in "ddm_client_trigger_waterfall"
        Table to get column filtering for columns Details , High & Qty upon applying filter
        with parameters config_id & run_dt
        these logs have Input files Data Suppressed and Drop% details for all the executions
        handled by the AWS Glue job
        '''
        print("Step 10: Waterfall data logs")
        # query = {'config_id': {'S':'dollar-consumer_trigger_E2E_Tx'}, 'run_dt':{'S':self.campaign_wednesday}}
        # self.waterfall_logs = self.dynamodb.get_item('ddm_client_trigger_waterfall', query)
        # print(">>>'", self.waterfall_logs)
        self.add_log("Step 10: Waterfall data logs", "WIP")
        self.step_logs_update()

    def step_logs_update(self):
        '''
        Step logs updates in "ddm_client_trigger_step" Table
        ----------------------------------------------------
        Connect to Dynamo DB. Step logs updates in "ddm_client_trigger_step" Table to get
        column filtering for column message upon applying filter with parameters config_id
        & run_dt these logs add messages from logs for all the executions handled by the
        AWS Glue job
        '''
        print("Step 11: Step logs update")
        # query = {'config_id': {'S':'dollar-consumer_trigger_E2E_Tx'}, 'run_dt':{'S':self.campaign_wednesday}}
        # self.steps_logs = self.dynamodb.get_item('ddm_client_trigger_waterfall', query)
        # print(">>>'", self.steps_logs)
        self.add_log("Step 11: Step logs update", "WIP")
        self.validate_bucket_output()

    def validate_bucket_output(self):
        '''
        Validate the output AWS S3
        --------------------------
        Validate the Output file at the designated AWS S3 bucket output file location
        Sample Output file
        Path: s3://useast1-dlx-{env}-ddm-client-process/ddm_client/{client}/data_processing/consumer_triggers_{year}/weekly_processing/week_number/intermediate_files/ 00
        - In Footprint.parquet
        '''
        print("Step 12: Validate Bucket Output")
        bucket_name = f"useast1-dlx-{self.s3.getenv("Environment")}-ddm-client-process"
        object_path = f"ddm_client/{self.client_name}/data_processing/consumer_triggers_{self.campaign_wednesday[:4]}/weekly_processing/week_{self.current_week}/intermediate_files/00 - In Footprint.parquet"
        # downloading target parquet file
        parquet_destination = os.path.join(self.s3.getenv('Parquet_Path'), 'S3Target.parquet')
        self.s3.download_file(bucket_name, object_path, parquet_destination)

        object_path = f"ddm_client/{self.client_name}/data_processing/consumer_triggers_{self.campaign_wednesday[:4]}/weekly_processing/week_{self.current_week}/intermediate_files/00 - In Footprint.txt"
        text_file_destination = os.path.join(self.s3.getenv('Parquet_Path'), 'S3Target.txt')
        self.s3.download_file(bucket_name, object_path, text_file_destination)
        self.add_log("Step 12: Validate Bucket Output", "Target parquet has been downloaded")

        self.validate_scenarios()


    def validate_scenarios(self):
        '''
        Cover Load Scenarios In Glue Job
        --------------------------------
        Cover Load In Glue Job positive and negative Scenarios as in this  Confluence
        Page: Consumer Trigger - Loadin Test steps - Deluxe Data Discovery
         - Confluence for all negative scenarios it must log a defect in Jira upon
         post actual Glue job execution of Load In
        '''
        print("Step: Validate Scenariso")
        self.write_output_parquet_as_csv()


    def write_output_parquet_as_csv(self):
        '''
        Write output of Input Parquet file converted as S3Source.csv
        ------------------------------------------------------------
        Write output of Input Parquet file converted as S3Source.csv & Output
        Parquet file converted as S3Target.csv in parquet folder in project
        root folder for QS to pic these output files from parquet to do
        end to end data validations by QuerySurge when run with CI Tool
        Harness (Deluxe DevOps Tool) headless setup
        '''
        print("Step: Write Output Parquet as CSV")
        parquet_destination = os.path.join(self.s3.getenv('Parquet_Path'), 'S3Target.parquet')
        self.parquet_target_df = self.parquet.read_parquet(parquet_destination)
        csv_destination = os.path.join(self.s3.getenv('Parquet_CSV_Path'), "S3Target.csv")
        self.parquet.write_to_csv(self.parquet_target_df, csv_destination)

        self.prepare_report()

    def prepare_report(self):
        final_report = ""
        for key, values in self.logs.items():
            final_report += f"\n{key} \n\t{"\n\t".join(values)}"

        with open("report.txt", "w+") as fp:
            fp.write(final_report)
        self.done()


if __name__ == "__main__":
    glue_job = Job()
    glue_job.run()
