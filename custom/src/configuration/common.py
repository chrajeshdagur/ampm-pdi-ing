import json
import logging
import sys
from datetime import datetime

import boto3
from botocore.config import Config
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

REGION = "eu-west-1"
S3 = boto3.client("s3")
DDB = boto3.resource(
    service_name="dynamodb",
    region_name=REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s",
)


class Utils(object):
    def __init__(self) -> None:
        self.logger = logging.getLogger(__class__.__name__)
        self.hwm_table: str = "high_watermark"

    def read_json(self, bucket: str, key: str) -> dict:
        try:
            self.logger.info(f"Reading file object from {bucket}/{key}")

            object_body = S3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

            data = json.loads(object_body)

            self.logger.info(f"File data {json.dumps(data)}")
            return {"status": True, "body": data}

        except Exception as e:
            self.logger.error(f"Reading object failed from s3://{bucket}/{key}: {str(e)}")
            return {"status": False, "error": e}

    def read_hwm_config(self, hwm_id: str, hwm_type: str) -> dict:
        table: str = self.hwm_table
        try:
            response = DDB.Table(table).get_item(Key={"hwm_id": hwm_id, "hwm_type": hwm_type})

            self.logger.info(f"High Water Mark {json.dumps(response['Item'])}")

            return {"status": True, "body": response["Item"]}

        except Exception as e:
            self.logger.error(f"Reading HWM for hwm_id={hwm_id} failed: {e}")

            return {"status": False, "error": None}

    def write_hwm_config(self, item: dict) -> None:
        table: str = "high_watermark"

        hwm_id: str = item["hwm_id"]
        hwm_type: str = item["hwm_type"]

        self.logger.info(f"Writing to HWM for hwm_id={hwm_id} and hwm_type={hwm_type}")
        try:
            serializer = TypeSerializer()
            deserializer = TypeDeserializer()
            serialized_item = serializer.serialize(item)
            deserialized_item = deserializer.deserialize(serialized_item)

            self.logger.info(f"HWM entry: {json.dumps(deserialized_item)}")

            # Update timestamp
            item["dh_audit_record_timestamp"] = datetime.now().strftime("%Y%m%d%H%M%S")
            DDB.Table(table).put_item(Item=item)

        except TypeError:
            self.logger.error(
                "HWM entry failed to deserialize. Convert ints and floats to strings before writing"
                " config to DDB."
            )
            raise

        except Exception as e:
            self.logger.error(
                f"Writing to HWM failed for hwm_id={hwm_id} and hwm_type={hwm_type}: {e}"
            )
            raise
    



