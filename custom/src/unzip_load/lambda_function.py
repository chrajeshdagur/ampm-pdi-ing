import csv
import io
import json
import logging
import os
import re
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta
from typing import List

import boto3
from botocore.config import Config

# Import utility functions from common.py
from common import Utils
from custom_json_logger import LambdaJsonLogFilter
from datalake_library import octagon

# Initialize logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s",
)

SUBTENANT: str = os.environ["SUBTENANT_ID"]
PIPELINE: str = os.environ["PIPELINE_NAME"]
ENV: str = os.environ["ENV"]

REGION = "eu-west-1"
s3_client = boto3.client("s3")
s3_resource = boto3.resource('s3')
DDB = boto3.resource(
    service_name="dynamodb",
    region_name=REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

OCTAGON = octagon.OctagonClient().with_run_lambda(True).with_configuration_instance(ENV).build()

def lambda_handler(event: dict, context) -> dict:
    print(event)
    T: Transform = Transform(event=event, context=context)
    return T.apply()


class Transform:
    def __init__(self, event=None, context=None):

        self.logger = logging.getLogger(__class__.__name__)
        self.logger.info("Reading: feed configurations")
        self.peh_id =  event["peh_id"]
        self.hwm_id =  event["hwm_id"]
        self.hwm_type =  event["hwm_type"]
        self.glue_execution_class =  event["glue_execution_class"]
        self.source_keys_path =  event["source_keys_path"]
        self.num_keys =  event["num_keys"]
        self.unzip_file_list = []

        self.transform_bucket = os.environ["TRANSFORM_BUCKET"]
        self.artifact_bucket = os.environ["ARTIFACTS_BUCKET"]
        self.raw_bucket = os.environ["RAW_BUCKET"]
        self.location = f"{self.artifact_bucket}/{self.source_keys_path}"
        self.output_path = 'custom/main/mobconv/pdi/dimension_file_csv/business_date='

        self.log_component = "Unzip Lambda"

        # Read the JSON file source file
        response = s3_client.get_object(Bucket=self.artifact_bucket, Key=self.source_keys_path)
        self.source_keys = json.loads(response['Body'].read().decode('utf-8'))
        self.output_file_paths = []


    def apply(self):
        self.logger.info('Unzipping file')
        last_key = self.source_keys["keys"][-1]
        for source_key in self.source_keys["keys"]:
            filename = source_key.split('/')[-1]
            business_date = filename[:8]
            write_path = self.output_path + f'{business_date}'
            # Download the ZIP file from S3
            response = s3_client.get_object(Bucket=self.raw_bucket, Key=source_key)
            zip_data = response['Body'].read()

            # Unzip the file and save its contents to the output location
            keys: List[str] = self.unzip_and_save_to_s3(zip_data, self.transform_bucket, write_path)  # noqa: E501

            if len(keys) == 0:
                self.logger.info("No new raw data objects available")
                response = {
                    "peh_id": self.peh_id,
                    "hwm_id": self.hwm_id,
                    "hwm_type": self.hwm_type,
                    "test": "False",
                    "glue_execution_class": self.glue_execution_class,
                    "source_keys_path": "False",
                    "num_keys": 0,
                    "last_key": last_key,
                }
                return response

            else:
                self.logger.info("Saving S3 listing")
                source_keys_path: str = Utils.write_to_artifacts(
                    bucket=self.artifact_bucket,
                    peh_id=self.peh_id,
                    pipeline=PIPELINE,
                    subfolder="unzip_file",
                    data={"keys": keys},
                )

        return {
            "peh_id": self.peh_id,
            "hwm_id": self.hwm_id,
            "hwm_type": self.hwm_type,
            "test": "False",
            "glue_execution_class": self.glue_execution_class,
            "source_keys_path": source_keys_path,
            "num_keys": len(keys),
            "last_key": last_key
        }

    def extract_business_date(self, file_name):
        # Extract the date using regular expression pattern matching
        pattern = r"(\d{4})(\d{2})(\d{2})\w+\.csv"
        match = re.search(pattern, file_name)
        if match:
            year = match.group(1)
            month = match.group(2)
            day = match.group(3)
            date_str = f"{year}-{month}-{day}"
            return date_str

        return None

    def unzip_and_save_to_s3(self, zip_data, output_bucket, output_path):
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
            for file_name in zip_file.namelist():
                # Extract the business_date from the file name
                output_key = f"{output_path}/{os.path.basename(file_name)}"
                with tempfile.TemporaryFile() as temp_file:
                    # Extract and write the file in chunks
                    with zip_file.open(file_name) as zip_entry:
                        for chunk in iter(lambda: zip_entry.read(8192), b''):
                            temp_file.write(chunk)

                    # Reset the file pointer to the beginning of the temporary file
                    temp_file.seek(0)

                    # Upload the file from the temporary file to S3
                    s3_client.upload_fileobj(temp_file, output_bucket, output_key)

                self.output_file_paths.append(output_key)

        return self.output_file_paths

