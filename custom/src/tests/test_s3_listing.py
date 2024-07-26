from unittest.mock import patch

import pytest  # noqa: F401
from s3_listing.lambda_function import Transform


@patch("s3_listing.lambda_function.octagon")
def test_get_s3_objects_list(octagon, s3_client, ddb_resource, s3_bucket, utilities):
    raw_path = "thorntons_fna/thorntonsposdata/datahub_ds="
    items = [
        {  # item in different dataset, should not appear
            "Key": "dip/rawmashgin/datahub_ds=20230101/datahub_ts=120000/file_01.json",
            "Body": "test",
        },
        {  # item prior to datahub_ts, should not appear
            "Key": "dip/rawgrabango/datahub_ds=20230101/datahub_ts=010000/file_01.json",
            "Body": "test",
        },
        {
            "Key": f"{raw_path}20240223/datahub_ts=083051/20240223AMPMPDI_FULL.zip",
            "Body": "test",
        },
        {
            "Key": f"{raw_path}20240223/datahub_ts=220000/20240222AMPMPDI_FULL.zip",
            "Body": "test",
        },
        {  # control file does not match pattern, should not appear
            "Key": "dip/rawgrabango/datahub_ds=20230103/datahub_ts=120000/file_04.txt",
            "Body": "test",
        },
        {  # zero byte file, should not apppear
            "Key": "dip/rawgrabango/datahub_ds=20230103/datahub_ts=120000/file_05.json",
        },
        {  # outside date range, should not appear
            "Key": "dip/rawgrabango/datahub_ds=20230201/datahub_ts=110000/file_06.json",
            "Body": "test",
        },
    ]

    # test whole method , get sorted list of objects
    utilities.fill_bucket(client=s3_client, bucket=s3_bucket, items=items)

    input_event = {
        "requestId": "6b15d642-9b42-4b86-8dab-2ced23a400ab",
        "body": {
            "peh_id": "78589dd4-5737-48af-87b2-5c92b0485cd3",
            "hwm_id": "mobconv#ampm-pdi-ing#thorntons_fna#thorntonsposdata",
            "hwm_type": "custom",
            "test": False,
            "glue_execution_class": "FLEX",
            "config": {
                "country_code": "US",
                "database": "thorntons_fna",
                "max_objects": "5000",
                "source_system": "PDI",
                "delimiters": ",",
                "pattern": "(\\d{8})AMPMPDI_FULL.zip",
                "active": True,
                "max_days": "1",
                "datahub_ds": "20240223",
                "datahub_ts": "080000",
                "file_format": "csv",
                "table": "thorntonsposdata"
            }
        }
    }
    s3_lister = Transform(
        s3_client=s3_client, ddb=ddb_resource, context=None, event=input_event["body"]
    )
    key_list = s3_lister.build_source_keys()

    assert key_list == [
        "thorntons_fna/thorntonsposdata/datahub_ds=20240223/datahub_ts=083051/20240223AMPMPDI_FULL.zip",
        "thorntons_fna/thorntonsposdata/datahub_ds=20240223/datahub_ts=220000/20240222AMPMPDI_FULL.zip",
    ]
    octagon.OctagonClient.assert_called()
