from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from typing import List

import boto3
from botocore.config import Config
from common import Glue, Utils
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat_ws, substring, to_date, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window, WindowSpec

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s",
)

GLUE_ARGS = [
    "TENANT",
    "SUBTENANT",
    "ENV",
    "ARTIFACTS_BUCKET",
    "RAW_BUCKET",
    "TRANSFORM_BUCKET",
    "HWM_TABLE",
    "PIPELINE",
    "PROJECT",
    "DATABASE",
    "TABLE",
    "TARGET_S3_PATH",
    "PEH_ID",
    "HWM_ID",
    "HWM_TYPE",
    "SOURCE_KEYS_PATH",
    "LAST_KEY"
]

class Transform:
    def __init__(self,
                 s3_client,
                 ddb_client,
                 glue_client,
                 glue,
                 utils
        ) -> None:
        self.logger = logging.getLogger(__class__.__name__)

        #Initialize glue
        self.glue = glue

        #Initialize utilities
        self.utilities = utils

        self.config: dict = self.glue.config

        self.logger.info(json.dumps(self.config, indent=2))
        self.raw_bucket: str = self.config["raw_bucket"]
        self.transform_bucket: str = self.config["transform_bucket"]
        self.job_id: str = self.config["peh_id"]
        self.hwm_id: str = self.config["hwm_id"]
        self.hwm_type: str = self.config["hwm_type"]
        self.last_key: str = self.config["last_key"]

        self.creation_datetime = datetime.now()

        # Datahub-Config for pipeline static configuration
        #_response: dict = UTILS.read_datahub_config()
        _response: dict = self.utilities.read_json(
                bucket=self.config["artifacts_bucket"],
                key="ampm-pdi-ing/config/datahub_config.json"
            )
        self.datahub_config: dict = _response["body"]

        # High Water Mark for Predicates
        _response: dict = self.utilities.read_hwm_config(
            hwm_id=self.config["hwm_id"], hwm_type=self.config["hwm_type"]
        )
        if not _response["status"]:
            raise Exception(_response["error"])
        self.hwm: dict = _response["body"]

        source_config: dict = self.hwm["source_config"][0]
        self.raw_source: dict = source_config["sources"]["raw"]
        self.targets: dict = source_config["targets"]
        self.debug: bool = source_config["common"].get("debug", False)
        self.test: bool = source_config["common"].get("test", False)
        self.use_prod_data: bool = source_config["common"].get("use_prod_data", False)

        self.s3_prefix: str = f"s3://{self.config['transform_bucket']}/custom/main/{self.config['subtenant']}/pdi/dimension_tables_parquet/"
        if self.test:
            self.s3_prefix: str = f"{self.s3_prefix}_tmp"

    def raw_file(self, key) -> dict:
        """Build Spark DataFrame based on objects in raw bucket.
           Revise logic based on individual use case to parse
           raw bucket objects.

           Source objects are CSV files with Pipe Delimiters
           with Header information.

        Returns:
            dict: With status and DataFrame
        >>> {
            "status": True | False,
            "data": <spark dataframe>
        }
        """
        config: dict = self.raw_source
        # Read schema from Datahub Config
        filename = key.split('/')[-1]
        self.logger.info(f"Processing File : {filename}")
        business_date = filename[:8]
        self.logger.info(f"Processing for business_date : {business_date}")
        datahub_ds = filename[:8]
        business_date = datetime.strptime(business_date, '%Y%m%d').strftime('%Y-%m-%d')
        filename = filename[8:]
        file_type = filename.split('.')[0]
        schema: StructType = StructType.fromJson(
            self.datahub_config["sources"]["raw"][file_type]["schema"])
        if not config["active"]:
            # do not process anything if source status is false
            return {"status": False, "data": None}

        # get list of source files from artifact bucket
        # log number and first 10 and last 10
        # read all in parallel rather than individually (pass list of keys to read method)
        # perform transformations as normal
        # how to get the file data into the rows w/o reading one at a time??
        # F.input_file_name() for each row instead!!!

        self.logger.info(f"Reading key: {key}"
        )
        response: dict = self.utilities.read_csv_files(
            bucket=self.transform_bucket,
            keys=[key],
            schema=schema,
            header=True,
            sep=config["delimiters"],
        )

        if not response["status"]:
            return {"status": False, "data": None}

        data: DataFrame = response["data"]
        current_timestamp = datetime.now().strftime("%H%M%S")
        source = "AMPM"
        data = (
            data.withColumn("input_file_name", F.lit(key.split('/')[-1]))
            .withColumn("job_id", F.lit(self.job_id))
            .withColumn("creation_datetime", F.lit(self.creation_datetime))
            .withColumn("datahub_ds", F.lit(datahub_ds))
            .withColumn("datahub_ts", F.lit(current_timestamp))
            .withColumn("business_date", F.lit(business_date))
            .withColumn("source", F.lit(source))
        )

        return {"status": True, "data": data}

    def update_hwm_config(self, data: DataFrame) -> None:
        """
        Updates the hmw_value based on max datahub_ds from the dataframe
        """
        hwm_config: dict = self.raw_source

        # get last timestamp of processed data and update HWM config for datahub_ds
        datahub_ds: str = data.agg({"datahub_ds": "max"}).collect()[0][0]
        self.logger.info(f"Max datahub_ds: {datahub_ds}")
        hwm_config["datahub_ds"] = datahub_ds

        # get last timestamp of processed data and update HWM config for datahub_ts
        datahub_ts: str = data.agg({"datahub_ts": "max"}).collect()[0][0]
        hwm_config["datahub_ts"] = datahub_ts
        #Correct ds and ts
        datahub_ds = self.last_key.split('/')[2]
        datahub_ds = datahub_ds.split('=')[1]
        datahub_ts = self.last_key.split('/')[3]
        datahub_ts = datahub_ts.split('=')[1]
        self.hwm['source_config'][0]['sources']['raw']['datahub_ds'] = datahub_ds
        self.hwm['source_config'][0]['sources']['raw']['datahub_ts'] = datahub_ts

    def new_dataset(self, key) -> dict:
        file_response: dict = self.raw_file(key)
        if not file_response["status"]:
            self.logger.info("No new fact data to process, ending job")
            return {"status": False, "data": self.utilities.empty_df}
        data = file_response ["data"]
        if self.debug:
            data.printSchema()
            data.show()

        response: dict = {"status": True, "data": data}

        return response

    def apply(self):
        """
        Main method to orchestrate data unzipping, transformation and writing
        """

        if self.config["source_keys_path"] == "False":
            self.logger.info("No new data to process, ending job")
            return False
        else:
            # get the actual keys from the artifact bucket
            _response: dict = self.utilities.read_json(
                bucket=self.config["artifacts_bucket"],
                key=self.config["source_keys_path"],
                print_content=False,
            )
            if not _response["status"]:
                raise Exception(_response["error"])
            self.config["keys"]: List[str] = _response["body"]["keys"]

            valid_patterns = [
                'GL_Accounts_AMPM.csv',
                'GL_Account_Majors_AMPM.csv',
                'GL_Account_Types_AMPM.csv',
                'Item_Brands_AMPM.csv',
                'Item_Attributes_AMPM.csv' ,
                'Item_Attribute_Dates_AMPM.csv' ,
                'Item_Attrib_ValueItemDetails_AMPM.csv' ,
                'Item_Attrib_ValueGroups_AMPM.csv' ,
                'Item_Group_Levels_AMPM.csv',
                'Item_Group_Membership_AMPM.csv',
                'Item_Groups_AMPM.csv',
                'Item_Manufacturers_AMPM.csv',
                'Item_Packages_AMPM.csv',
                'item_sizes_AMPM.csv',
                'Item_Standard_Info_AMPM.csv',
                'Item_UPCs_AMPM.csv',
                'items_AMPM.csv',
                'Profit_Centers_AMPM.csv',
                'Retail_Packages_AMPM.csv',
                'Retail_Prices_AMPM.csv',
                'Sites_AMPM.csv',
                'Vendor_CostZoneSites_AMPM.csv',
                'vendor_items_AMPM.csv',
                'Vendor_Item_Attributes_AMPM.csv',
                'Vendors_AMPM.csv',
                'Vendor_Costs_AMPM.csv',
                'Paperwork_Batches_AMPM.csv'
            ]
            valid_files = []
            for pattern in valid_patterns:
                keep = [file for file in self.config["keys"] if pattern in file]
                valid_files += keep
            self.config["keys"] = valid_files

            # Read new data from source tables
            for key in self.config["keys"]:
                try:
                    new_dataset_response: dict = self.new_dataset(key)
                except Exception as e:
                    print(f"Error processing key '{key}': {e}")
                    continue  # Skip this key and continue with the next one

                data: DataFrame = new_dataset_response["data"]

                # Write to S3
                file_type: str = key.split('/')[-1]
                business_date = file_type[:8]
                business_date = datetime.strptime(business_date, '%Y%m%d').strftime('%Y-%m-%d')
                file_type = file_type[8:]
                file_type = file_type.split('.')[0]

                hwm_config: dict = self.targets[file_type]
                datahub_config: dict = self.datahub_config["targets"][file_type]

                write_output = f"{self.s3_prefix}{file_type.lower()}/"

                response: dict = self.utilities.s3_sink(
                    data=data,
                    mode="overwrite",
                    partitions=datahub_config["partition_cols"],
                    path=write_output,
                    object_per_partition=int(hwm_config["object_per_partition"]),
                    schema=datahub_config["schema"]
                )

            # Update HWM config
            self.update_hwm_config(data)
            self.logger.info(self.hwm)

            # Update High Water Mark Table
            if response["status"]:
                self.utilities.write_hwm_config(item=self.hwm)

            return True


if __name__ == "__main__":
    S3 = boto3.client("s3")
    DDB = boto3.resource(
        service_name="dynamodb",
        region_name="eu-west-1",
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )
    GLUE = boto3.client(service_name="glue", region_name="eu-west-1")
    Glue = Glue(args=GLUE_ARGS)
    Utils = Utils(
        context=Glue.context,
        env=Glue.config["env"],
        subtenant=Glue.config["subtenant"],
        pipeline=Glue.config["pipeline"],
        s3_client = S3,
        ddb_client = DDB,
        glue_client = Glue
    )
    Transform(s3_client = S3, ddb_client = DDB, glue_client = GLUE, glue = Glue, utils = Utils).apply()
