import json
import logging
import os
import re
import sys
import traceback
from datetime import datetime, timedelta
from typing import List

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s",
)


class Utils(object):
    def __init__(self, s3_client, ddb) -> None:
        self.logger = logging.getLogger(__class__.__name__)
        self.s3_client = s3_client
        self.ddb = ddb

    def list_s3_objects(
        self,
        bucket: str,
        database: str,
        table: str,
        datahub_ds: str,
        datahub_ts: str,
        pattern: str,
        max_days: int,
        max_objects: int,
    ) -> List[str]:
        """Get list of all objects from S3 bucket for a prefix
           Ignore Zero Byte size objects

        Args:
            bucket (str): S3 bucket name
            source (str): database/table_
            datahub_ds (str): Date stamp yyyyMMdd
            datahub_ts (str): Time stamp HHmmss
            patter (str): RegEx pattern to qualify objects

        Returns:
            list[str]: List of qualified objects
        """
        logger = logging.getLogger(__class__.__name__)
        logger.setLevel("INFO")

        max_days = max_days if max_days else 1
        _max_objects = max_objects if max_objects else 1

        # create a list of datahub_ds to check
        list_datahub_ds: List[str] = Utils.date_range(
            from_date=datahub_ds, date_format="%Y%m%d", add_days=max_days
        )
        logger.info(f"{list_datahub_ds}")

        objects: List[str] = []
        for _datahub_ds in list_datahub_ds:
            if _datahub_ds == datahub_ds:
                # check for a datahub_ts
                objects += self.__list_s3_objects(
                    bucket=bucket,
                    source=f"{database}/{table}",
                    datahub_ds=_datahub_ds,
                    datahub_ts=datahub_ts,
                    pattern=pattern,
                    max_objects=_max_objects,
                )
            else:
                # check all datahub_ts
                objects += self.__list_s3_objects(
                    bucket=bucket,
                    source=f"{database}/{table}",
                    datahub_ds=_datahub_ds,
                    datahub_ts=None,
                    pattern=pattern,
                    max_objects=_max_objects,
                )

            # check if number max objects reached
            _max_objects = max_objects - len(objects)
            if _max_objects <= 0:
                break

        return sorted(list(set(objects)))

    @staticmethod
    def date_range(from_date: str, date_format: str, add_days: int) -> List[str]:
        """Create a list of Dates based in format

        Args:
            from_date (str): From date
            date_format (str): date format
            add_days (int): Total Days

        Returns:
            List[str]: List of dates as string
        """

        dates: List[str] = []

        base_date = datetime.strptime(from_date, date_format)
        current_date: datetime = datetime.now()

        for x in range(add_days):
            _date: datetime = base_date + timedelta(days=x)

            # don't include future dates
            if _date > current_date:
                break
            else:
                dates.append(_date.strftime(date_format))

        return dates

    def __list_s3_objects(
        self,
        bucket: str,
        source: str,
        pattern: str,
        datahub_ds: str,
        datahub_ts: str,
        max_objects: int,
    ) -> List[str]:
        """Get list of all objects from S3 bucket for a prefix
           Ignore Zero Byte size objects
           Filter objects based on regex pattern, allow all if its None
           Limit total objects based on max_objects limit
           Limit objects based on datahub_ds and datahub_ts

        Args:
            bucket (str): S3 bucket name
            source (str): database/table_
            datahub_ds (str): Date stamp yyyyMMdd
            datahub_ts (str): Time stamp HHmmss
            patter (str): RegEx pattern to qualify objects

        Returns:
            list[str]: List of qualified objects
        """

        logger = logging.getLogger(__class__.__name__)
        logger.setLevel("INFO")

        keys: list[str] = []
        prefix = f"{source}/datahub_ds={datahub_ds}/"

        try:
            logger.info(
                f"Get list of objects for s3://{bucket}/{prefix} limit {max_objects} objects"
            )

            is_more_objects: bool = True
            token = None

            while is_more_objects:
                if token is None:
                    response: dict = self.s3_client.list_objects_v2(
                        Bucket=bucket, Prefix=prefix, MaxKeys=max_objects
                    )
                else:
                    response: dict = self.s3_client.list_objects_v2(
                        Bucket=bucket, Prefix=prefix, MaxKeys=max_objects, ContinuationToken=token
                    )

                is_more_objects = response["IsTruncated"]
                token = response.get("NextContinuationToken", None)

                if response.get("KeyCount", 0) > 0:
                    for content in response["Contents"]:
                        if Utils.validate_object(
                            object=content,
                            pattern=pattern,
                            datahub_ts=datahub_ts,
                        ):
                            keys.append(content["Key"])

                if len(keys) >= max_objects:
                    # do not need to retrieve more objects
                    return Utils.limit_objects(keys=keys, max_objects=max_objects)

            return keys

        except Exception as e:
            logger.error(f"Failed to list object from s3://{bucket}/{prefix}/*")
            logger.error(f"Error message: {e}")
            print(traceback.format_exc())
            return []

    @staticmethod
    def limit_objects(keys: List[str], max_objects: int) -> List[str]:
        """Return array of keys by limiting based on max_objects
           Keys must be sorted.

        Args:
            keys (List[str]): String array with Key values
            max_objects (int): No of objects to limit

        Returns:
            List[str]: _description_
        """

        if len(keys) <= max_objects:
            return keys

        # limit keys based on prefix values
        last_object_key_prefix = "/".join(keys[max_objects - 1].split("/")[:4])

        left_keys: List[str] = keys[:max_objects]
        right_keys: List[str] = [key for key in keys[max_objects:] if last_object_key_prefix in key]

        return left_keys + right_keys

    @staticmethod
    def validate_object(object: dict, pattern: str, datahub_ts: str) -> bool:
        """Validate S3 Object based on content of S3 listing

        Args:
            object (dict): Content of S3 listing object
            pattern (str): RegEx pattern to filter object
            datahub_ts (str): Prefix to filter object

        Returns:
            bool: True when below three criteria valid otherwise False
                1. RegEx pattern match, when provided
                2. Object is more than zero byte in size
                3. Prefix is greater in value, when provided
        """

        # object must match regex pattern if provided; otherwise ignore
        if pattern:
            is_valid = re.search(pattern=pattern, string=object["Key"]) is not None
        else:
            is_valid = True

        # object must be > zero byte in size
        is_valid = False if object["Size"] <= 0 else is_valid

        # retrieve prefix information for object key
        _datahub_ts: str = object["Key"].split("/")[3].split("=")[-1]

        # object prefix must be after datahub_ts if provided; otherwise include
        is_valid = False if datahub_ts and _datahub_ts <= datahub_ts else is_valid

        return is_valid

    @staticmethod
    def create_batches(objects: List, batch_size: int) -> dict:
        """Split objects into multiple batches

        Args:
            objects (List): List of objects
            batch_size (int): Total objects per batch

        Returns:
            dict (str, List): batch details
        """

        total_items: int = len(objects)

        # Calculate total batches
        batches: int = int(total_items / batch_size)
        if batches * batch_size < total_items:
            batches += 1

        response: dict = {}

        start_index: int = 0
        for batch in range(batches):
            end_index: int = start_index + batch_size
            if end_index >= total_items:
                end_index = total_items

            batch_id: str = f"batch_{batch+1}"

            response[batch_id] = objects[start_index:end_index]
            start_index = end_index

        return response

    def write_to_artifacts(
        self,
        bucket: str,
        peh_id: str,
        pipeline: str,
        subfolder: str,
        data: dict
    ) -> str:
        """Write the python dictionary object to
        Artifacts bucket
        """
        local_file = f"/tmp/{peh_id}.json"
        target_file = f"{pipeline}/{subfolder}/{peh_id}.json"

        with open(local_file, "w") as F:
            json.dump(data, F)

        self.s3_client.upload_file(Filename=local_file, Bucket=bucket, Key=target_file)

        os.remove(local_file)

        return target_file
