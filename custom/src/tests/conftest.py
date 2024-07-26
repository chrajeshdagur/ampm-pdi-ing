import json
import os
import pathlib
import sys
from unittest.mock import MagicMock

import boto3
import pyspark.sql.types as T
import pytest
from botocore.config import Config
from moto import mock_dynamodb, mock_glue, mock_s3
from pyspark.sql import SparkSession

REGION = 'eu-west-1'
os.environ["RAW_BUCKET"] = "WS-00-test-bucket"
os.environ["SUBTENANT_ID"] = "mobconv"
os.environ["PIPELINE_NAME"] = "ampm-pdi-ing"
os.environ["ENV"] = "dev"
os.environ["ARTIFACTS_BUCKET"] = "WS-00-artifact-bucket"

sys.argv.append('--TENANT=dst')
sys.argv.append('--SUBTENANT=mobconv')
sys.argv.append('--ENV=test')
sys.argv.append('--ARTIFACTS_BUCKET=WS-00-artifact-bucket')
sys.argv.append('--RAW_BUCKET=WS-00-test-bucket')
sys.argv.append('--TRANSFORM_BUCKET=WS-00-transform-bucket')
sys.argv.append('--HWM_TABLE=high_watermark')
sys.argv.append('--PIPELINE=ampm-pdi-ing')
sys.argv.append('--PROJECT=thorntons')
sys.argv.append('--DATABASE=transform_main_mobconv_pdi')
sys.argv.append('--TABLE=daily_adjustments_details')
sys.argv.append('--TARGET_S3_PATH=custom/main/mobconv')
sys.argv.append('--PEH_ID=test_id')
sys.argv.append('--HWM_ID=mobconv#ampm-pdi-ing#thorntons_fna#thorntonsposdata')
sys.argv.append('--HWM_TYPE=string')
sys.argv.append('--SOURCE_KEYS_PATH=keys')
sys.argv.append('--LAST_KEY=keys')

#Configure global variables here
def pytest_configure():
    """Global variables used across multiple test files
    """
    pytest.region = 'eu-west-1'
    pytest.test_event_1 = {
    'raw_bucket': 'WS-00-raw-bucket',
    'transform_bucket': 'transformbucket',
    'subtenant': 'mobconv',
    'pipeline': 'ampm-pdi-ing',
    'peh_id': 'test_peh_id',
    'env': 'dev',
    'source': 'pdi',
    'hwm': {
        "hwm_id" : "mobconv#ampm-pdi-ing#thorntons_fna#thorntonsposdata",
        "hwm_type" : "custom",
        "source_config": {
            "version": "1",
            "latest_datahub_ds" : "20240223",
            "latest_datahub_ts" : "083112",
            "process_history_flag" : "False"
        },
        "sub_zone": 'main'
    },
    'keys': '<keys>'
}
    pytest.hwm_dict = {
        "hwm_id" : "mobconv#ampm-pdi-ing#thorntons_fna#thorntonsposdata",
        "hwm_type" : "custom"
    }


@pytest.fixture()
def s3_client():
    """Mock S3 Client generator object

    Args: None

    Returns:
        S3 Client Object
    """
    with mock_s3():
        s3 = boto3.client("s3")
        yield s3

@pytest.fixture()
def ddb_resource():
    """Mock DDB Resource generator object

    Args: None

    Returns:
        DDB Resource Object
    """
    with mock_dynamodb():
        ddb = boto3.resource(service_name='dynamodb', region_name=REGION, config=Config(
    retries={'max_attempts': 10, 'mode': 'standard'}))
        yield ddb

@pytest.fixture()
def glue_client():
    with mock_glue():
        glue = boto3.client(service_name="glue", region_name="eu-west-1")
        yield glue

@pytest.fixture()
def s3_bucket(s3_client):
    """Creates a bucket within mock s3 client.

    Args:
        Mock s3_client object

    Returns:
        string: bucket name
    """
    bucket_name = "WS-00-test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name

@pytest.fixture()
def s3_bucket_raw(s3_client):
    """Creates a bucket within mock s3 client.

    Args:
        Mock s3_client object

    Returns:
        string: bucket name
    """
    bucket_name = "WS-00-raw-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name

@pytest.fixture()
def s3_bucket_artifact(s3_client):
    """Creates a bucket within mock s3 client.

    Args:
        Mock s3_client object

    Returns:
        string: bucket name
    """
    bucket_name = "WS-00-artifact-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name

@pytest.fixture()
def s3_bucket_transform(s3_client):
    """Creates a bucket within mock s3 client.

    Args:
        Mock s3_client object

    Returns:
        string: bucket name
    """
    bucket_name = "WS-00-transform-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name

@pytest.fixture()
def ddb_table(ddb_resource):
    """Creates high watermark table within mock ddb object.

    Args:
        Mock ddb_resource object

    Returns:
        string: table name
    """
    ddb_resource.create_table(
        TableName = 'high_watermark',
        KeySchema = [
            {
                'AttributeName': 'hwm_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'hwm_type',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'hwm_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'hwm_type',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return 'high_watermark'

@pytest.fixture()
def utilities():
    return Utility

@pytest.fixture()
def schemabuilder():
    return SchemaBuilder

@pytest.fixture()
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("PySpark unit test")
        .getOrCreate()
    )
    yield session

@pytest.fixture()
def glue(spark):
    glue = MagicMock()
    glue.config = {
        "raw_bucket": "WS-00-test-bucket",
        "transform_bucket": "WS-00-transform-bucket",
        "artifacts_bucket": "WS-00-artifact-bucket",
        "peh_id": "test_peh",
        "hwm_id": "mobconv#ampm-pdi-ing#thorntons_fna#thorntonsposdata",
        "hwm_type": "custom",
        "subtenant": "mobconv",
        "project": "thorntons",
        "last_key" : "keys",
    }
    glue.spark_session = spark
    yield glue

def load_json_config(filename):
    config_path = (
        pathlib.Path(__file__).parent.parent.parent / "config" / filename
    ).resolve()
    with open(config_path) as config_file:
        config_content = json.loads(config_file.read())
        return config_content

@pytest.fixture
def datahub_config():
    # load in actual datahub config json
    return {"status": True, "body": load_json_config("datahub_config.json")}


@pytest.fixture
def hwm_config():
    # load in actual hwm config json
    return {"status": True, "body": {"source_config": [load_json_config("hwm.json")]}}

@pytest.fixture
def utils(spark, datahub_config, hwm_config):
    utils = MagicMock()
    utils.spark_session = spark
    utils.read_datahub_config.return_value = datahub_config
    utils.read_hwm_config.return_value = hwm_config
    yield utils

class Utility:

    @staticmethod
    def read_json(filename):
        f = open(filename, "r")
        data = json.load(f)
        f.close()
        return data

    @staticmethod
    def fill_bucket(client, bucket, items):
        for i in items:
            client.put_object(Bucket=bucket, **i)

    @staticmethod
    def fill_table(resource, table, items):
        for i in items:
            resource.Table(table).put_item(TableName=table, **i)

    @staticmethod
    def get_csv_path(test_rank):
        config_path = ( pathlib.Path(__file__).parent.parent.parent / "testdata") \
            .resolve()

        all_files = [file for file in os.listdir(config_path) if str(test_rank) in file]
        mapping_dict = {}
        for file  in all_files:
            if '.csv' in file and 'result' not in file:
                mapping_dict['csv'] =  os.path.join(config_path, file)
            if '.json' in file:
                mapping_dict['json'] = os.path.join(config_path, file)
            if '.csv' and 'result' in file:
                mapping_dict['result'] = os.path.join(config_path, file)

        return mapping_dict

    @staticmethod
    def get_file_path(filename):
        file_path = (pathlib.Path(__file__).parent / 'testdata' / filename).resolve()
        return str(file_path)

    @staticmethod
    def read_schema_column_json(filepath):
        with open(Utility.get_file_path(filepath)) as f:
            config_dict = json.load(f)
        field_names = list(config_dict.keys())
        return config_dict, field_names

class SchemaBuilder:

    mapping = {
        'string' : 'StringType',
        'bool' : 'BooleanType',
        'double': 'DoubleType',
        'timestamp': 'TimestampType',
        'date' : 'DateType'
    }

    def __init__(self):
        return

    @staticmethod
    def create_schema(config_dict):
        field_names = list(config_dict.keys())
        field_name = field_names[0]

        data_type = SchemaBuilder.mapping[config_dict[field_name]['type']]
        datatype_func = getattr(T, data_type)
        nullable = config_dict[field_name]['nullable']
        schema = T.StructType([T.StructField(field_name, datatype_func(), nullable)])

        for i in range(1, len(field_names)):
            field_name = field_names[i]
            data_type = SchemaBuilder.mapping[config_dict[field_name]['type']]
            datatype_func = getattr(T, data_type)
            nullable = config_dict[field_name]['nullable']
            schema.add(field_name, datatype_func(), nullable)

        return schema
