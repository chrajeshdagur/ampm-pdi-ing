import json
import logging
import os
import sys
from datetime import datetime

from common import Utils
from custom_json_logger import LambdaJsonLogFilter
from datalake_library import octagon

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    encoding="utf-8",
    format="%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s",
)

TENANT: str = os.environ["TENANT_ID"]
SUBTENANT: str = os.environ["SUBTENANT_ID"]
PIPELINE: str = os.environ["PIPELINE_NAME"]
ENV: str = os.environ["ENV"]
ARTIFACTS_BUCKET: str = os.environ["ARTIFACTS_BUCKET"]
HWM_TABLE: str = os.environ["HWM_TABLE"]
HWM_TYPE: str = "custom"

UTILS: Utils = Utils()

OCTAGON = octagon.OctagonClient().with_run_lambda(True).with_configuration_instance(ENV).build()


def lambda_handler(event: dict, context) -> dict:
    """This function build configuration.

    - Read configuration object from `artifacts_bucket/../config.json`
    - `Raise Exception` when object failed to read from Artifacts Bucket
    - Build `hwm_id` from Artifacts Config when missing in event object
    - Create default `HWM` Config based on Artifacts Config when HWM missing
    - Read configuration from High Water Mark Table

    ### Args:
        event (dict):
    >>> {
            "peh_id": "<peh_id>"
        }

    ### Returns:
        dict: event object with config information.
        `source_config` would be based on individual use case.
    >>> {
            "peh_id": "<peh_id>",
            "hwm_id": "<hwm_id>",
            "hwm_type": "<HWM_TYPE>",
            "test": <true|false>,
            "config": {
                "active": <true|false>,
                "source_system": "<source_system>",
                "country_code": "<country_code>",
                "pattern": "<regex_pattern>",
                "database": "<source_folder>",
                "table": "<source_feed_folder>",
                "datahub_ds": "<yyyyMMdd>",
                "datahub_ts": "<HHmmss>",
                "max_days": "<20>",
                "max_objects": "<20>",
                "delimiters": "|"
            }
        }
    """
    T: Transform = Transform(event=event, context=context)

    return T.apply()


class Transform(object):
    def __init__(self, event: dict, context) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel("INFO")

        self.notification = LambdaJsonLogFilter.pre_init(context).set_logger(self.logger)

        self.peh_id: str = event.get("peh_id", context.aws_request_id)
        self.hwm_id: str = event.get("hwm_id", None)
        self.config: dict = {}

    def reset_notification(self) -> None:
        self.notification.reset(subtenant=SUBTENANT, pipeline=PIPELINE, peh_id=self.peh_id)

        self.notification.set_additional_info("NOTE", "Scheduler for Latest Transform")

    def read_artifact_config(self) -> dict:
        self.logger.info(f"Read configuration from {ARTIFACTS_BUCKET}")
        config_file: str = f"{PIPELINE}/config/hwm.json"
        artifacts_config_response: dict = UTILS.read_json(bucket=ARTIFACTS_BUCKET, key=config_file)

        if not artifacts_config_response["status"]:
            raise ValueError(
                f"Configuration could not be read from {ARTIFACTS_BUCKET}/{config_file}"
            )

        artifacts_config: dict = artifacts_config_response["body"]

        return artifacts_config

    def get_hwm_config_default(self, config: dict) -> dict:
        hwm_attribute: str = "custom"

        self.logger.info(f"Using config from Artifact Bucket")
        hwm_config: dict = {
            "hwm_id": self.hwm_id,
            "hwm_type": HWM_TYPE,
            "tenant_id": TENANT,
            "tenant_env": ENV,
            "subtenant_id": SUBTENANT,
            "zone": "transform",
            "sub_zone": "main",
            "hwm_value": "custom",
            "hwm_data_type": "string",
            "hwm_attribute": hwm_attribute,
            "dh_audit_record_timestamp": datetime.now().strftime("%Y%m%d%H%M%S"),
            "source_config": [
                {
                    "common": config.get("common", {}),
                    "sources": config.get("sources", {}),
                    "targets": config.get("targets", {}),
                }
            ],
        }

        return hwm_config

    def set_hwm_id(self, config: dict) -> None:
        if self.hwm_id is None:
            self.logger.info(f"Create hwm_id form Artifact Config")
            database: str = config["sources"]["raw"]["database"]
            table: str = config["sources"]["raw"]["table"]
            self.hwm_id: str = f"{SUBTENANT}#{PIPELINE}#{database}#{table}"

    def build_hwm_config(self) -> dict:
        # Read config from artifact bucket
        artifacts_config: dict = self.read_artifact_config()
        # Build hwm id string
        self.set_hwm_id(config=artifacts_config)
        # Use hwm id to check for DDB entry
        self.logger.info(f"Read configuration from {HWM_TABLE} for {self.hwm_id}")
        hwm_config_response: dict = UTILS.read_hwm_config(hwm_id=self.hwm_id, hwm_type=HWM_TYPE)

        if hwm_config_response["status"]:
            hwm_config: dict = hwm_config_response["body"]

        else:
            hwm_config: dict = self.get_hwm_config_default(config=artifacts_config)

        return hwm_config

    def apply(self) -> dict:
        log_component = "HWM registration lambda"
        log_msg = "Reading HWM DDB entry"

        octagon.peh.PipelineExecutionHistoryAPI(OCTAGON).retrieve_pipeline_execution(self.peh_id)

        OCTAGON.update_pipeline_execution(component=log_component, status=log_msg)

        self.reset_notification()

        self.config: dict = self.build_hwm_config()

        UTILS.write_hwm_config(item=self.config)

        return {
            "peh_id": self.peh_id,
            "hwm_id": self.config["hwm_id"],
            "hwm_type": self.config["hwm_type"],
            "test": self.config["source_config"][0]["common"].get("test", False),
            "glue_execution_class": self.config["source_config"][0]["common"].get(
                "glue_execution_class", "STANDARD"
            ),
            "config": self.config["source_config"][0]["sources"]["raw"],
        }
