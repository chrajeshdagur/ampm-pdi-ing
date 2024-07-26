import logging
import os
import sys
from typing import List

import boto3
from botocore.config import Config
from common import Utils
from custom_json_logger import LambdaJsonLogFilter
from datalake_library import octagon

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    encoding="utf-8",
    format="%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s",
)


def lambda_handler(event: dict, context) -> dict:
    """Make a list of S3 object to be processed

     - List S3 objects based on  `s3:`//`bucket`/`source_folder`/`source_feed_folder`/
     `datahub_ds=yyyyMMdd`/`datahub_ts=HHmmss`/`*`
     - Filter objects based on `pattern`, `max_days` and `max_objects`
     - `Raise Exception` when zero object found
     - Exclude objects with zero byte size
     - Return List of List which include list of s3 `keys` per batch

    ### Args:
         event (dict):
     >>> {
             "peh_id": "<peh_ud>"
             "hwm_id": "<hwm_id>",
             "hwm_type": "<hwm_type>",
             "glue_execution_class": "<glue_execution_class>",
             "config":
                 {
                     "source_system": "<source_system>",
                     "country_code": "<country_code>",
                     "pattern": "<regex_pattern>",
                     "database": "<source_folder>",
                     "table": "<source_feed_folder>",
                     "datahub_ds": "<yyyyMMdd>",
                     "datahub_ts": "<HHmmss>",
                     "max_days": "<20>",
                     "max_objects": "<20>"
                 }
         }
         context (object): _description_

     ### Returns:
         List of S3 Objects without bucket name
     >>> {
             "peh_id": "<peh_ud>"
             "hwm_id": "<hwm_id>",
             "hwm_type": "<hwm_type>",
             "keys": ['key_1', 'key_2', ...]
         }
    """

    s3_client = boto3.client("s3")
    ddb = boto3.resource(
        service_name="dynamodb",
        region_name="eu-west-1",
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )

    T: Transform = Transform(
        event=event, context=context, s3_client=s3_client, ddb=ddb
    )  # noqa: E501

    return T.apply()


class Transform(object):
    def __init__(self, event, context, s3_client, ddb) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel("INFO")

        self.notification = LambdaJsonLogFilter.pre_init(context).set_logger(
            self.logger
        )  # noqa: E501
        self.Utils = Utils(s3_client=s3_client, ddb=ddb)

        self.subtenant = os.environ["SUBTENANT_ID"]
        self.pipeline = os.environ["PIPELINE_NAME"]
        self.env = os.environ["ENV"]
        self.raw_bucket = os.environ["RAW_BUCKET"]
        self.artifacts_bucket = os.environ["ARTIFACTS_BUCKET"]

        self.octagon = (
            octagon.OctagonClient()
            .with_run_lambda(True)
            .with_configuration_instance(self.env)
            .build()
        )  # noqa: E501

        self.peh_id: str = event.get("peh_id", None)
        self.hwm_id: str = event.get("hwm_id", None)
        self.hwm_type: str = event.get("hwm_type", None)
        self.test: bool = event.get("test", False)
        self.glue_execution_class: str = event.get("glue_execution_class", "STANDARD")
        self.config: dict = event["config"]
        self.database: str = self.config["database"]
        self.table: str = self.config["table"]
        self.pattern: str = self.config["pattern"]
        self.datahub_ds: str = self.config["datahub_ds"]
        self.datahub_ts: str = self.config["datahub_ts"]
        self.max_days: int = int(self.config["max_days"])
        self.max_objects: int = int(self.config["max_objects"])
        self.is_active: bool = self.config["active"]

        self.feed: str = f"{self.database}/{self.table}"
        self.log_component = "S3 Listing Lambda"

        octagon.peh.PipelineExecutionHistoryAPI(
            self.octagon
        ).retrieve_pipeline_execution(self.peh_id)

    def reset_notification(self) -> None:
        self.notification.reset(
            subtenant=self.subtenant, PIPELINE=self.pipeline, peh_id=self.peh_id
        )  # noqa: E501

        self.notification.set_additional_info("NOTE", "Scheduler for Latest Transform")

    def build_source_keys(self) -> List[str]:
        self.notification.set_additional_info("NOTE", "Listing of S3 Objects")

        self.logger.info(f"Creating S3 Object listing for {self.feed}")

        keys: List[str] = self.Utils.list_s3_objects(
            bucket=self.raw_bucket,
            database=self.database,
            table=self.table,
            datahub_ds=self.datahub_ds,
            datahub_ts=self.datahub_ts,
            pattern=self.pattern,
            max_days=self.max_days,
            max_objects=self.max_objects,
        )
        return keys

    def apply(self) -> dict:
        # Set to string value as glue job requires SOURCE_KEYS_PATH as an argument
        source_keys_path = "False"
        if self.is_active:
            self.octagon.update_pipeline_execution(
                component=self.log_component,
                status="Reading new source keys from raw bucket",  # noqa: E501
            )

            self.notification.set_additional_info("NOTE", "Listing of S3 Objects")

            self.logger.info(f"Creating S3 Object listing for {self.feed}")
            keys: List[str] = self.build_source_keys()

            if len(keys) == 0:
                self.logger.info("No new raw data objects available")
                self.octagon.update_pipeline_execution(
                    component=self.log_component,
                    status="No new files available in raw bucket",  # noqa: E501
                )

                # Raise an Exception here if you want your pipeline to stop when
                # there is no new data available in the raw bucket
                # raise Exception("No new data in raw bucket")

            else:
                self.logger.info("Saving S3 listing")
                keys.sort()
                source_keys_path: str = self.Utils.write_to_artifacts(
                    bucket=self.artifacts_bucket,
                    peh_id=self.peh_id,
                    pipeline=self.pipeline,
                    subfolder="s3_listings",
                    data={"keys": keys},
                )

                self.octagon.update_pipeline_execution(
                    component=self.log_component,
                    status=f"Num keys: {len(keys)}. First key: {keys[0]}. Last key: {keys[-1]}",  # noqa: E501
                )
        else:
            log_msg = (
                f"Raw dataset {self.database}/{self.table} is marked inactive, skipping object"  # noqa: E501
                " listing"
            )
            self.logger.info(log_msg)
            self.octagon.update_pipeline_execution(
                component=self.log_component,
                status=log_msg,
            )

        return {
            "peh_id": self.peh_id,
            "hwm_id": self.hwm_id,
            "hwm_type": self.hwm_type,
            "test": self.test,
            "glue_execution_class": self.glue_execution_class,
            "source_keys_path": source_keys_path,
            "num_keys": len(keys),
        }
