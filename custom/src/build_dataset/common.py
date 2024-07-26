import json
import logging
import sys
from datetime import datetime, timedelta
from functools import reduce
from typing import List, Tuple

from awsglue.context import GlueContext, SparkSession
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.config import Config
from dateutil.relativedelta import relativedelta
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    Row,
    StringType,
    StructType,
)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s",
)


class Utils(object):
    def __init__(self,
                 context: GlueContext,
                 env: str,
                 subtenant: str,
                 pipeline: str,
                 s3_client,
                 ddb_client,
                 glue_client) -> None:
        self.logger = logging.getLogger(__class__.__name__)
        self.s3_client = s3_client
        self.ddb_client = ddb_client
        self.env: str = env
        self.subtenant: str = subtenant
        self.pipeline: str = pipeline
        self.glue_context: GlueContext = context
        self.spark_session: SparkSession = context.spark_session
        self.empty_df: DataFrame = self.spark_session.createDataFrame([], StructType([]))

        self.datahub_table: str = f"datahub-config-{self.env}"
        self.hwm_table: str = "high_watermark"

    def read_csv(
        self, bucket: str, key: str, schema: StructType, header: bool, sep: str = "|"
    ) -> dict:
        """_summary_

        Args:
            bucket (str): Source bucket name
            key (str): Object key with prefix
            schema (StructType): Input Spark Dataframe Schema
            sep (str, optional): Delimiters. Defaults to "|".

        Returns:
            dict: Dataframe with status
        """

        object_path: str = f"s3://{bucket}/{key}"
        try:
            self.logger.info(f"Reading CSV file: {object_path}")
            data: DataFrame = (
                self.spark_session.read.format("CSV")
                .schema(schema=schema)
                .option("sep", sep)
                .option("header", header)
                .load(object_path)
            )

            return {"status": True, "data": data}

        except Exception as e:
            self.logger.error(f"Failed to read CSV file {object_path}: {str(e)}")
            return {"status": False, "data": self.empty_df}

    def read_csv_files(
        self,
        bucket: str,
        keys: List[str],
        schema: StructType,
        header: bool,
        sep: str = "|",
        **kwargs,
    ) -> dict:
        """_summary_

        Args:
            bucket (str): Source bucket name
            key (str): Object key with prefix
            schema (StructType): Input Spark Dataframe Schema
            sep (str, optional): Delimiters. Defaults to "|".

        Returns:
            dict: Dataframe with status
        """

        object_paths: List[str] = [f"s3://{bucket}/{key}" for key in keys]
        try:
            self.logger.info(
                f"Reading {len(object_paths)} CSV files. First file: {object_paths[0]}. Last file:"
                f" {object_paths[-1]}"
            )
            data: DataFrame = (
                self.spark_session.read.format("CSV")
                .schema(schema=schema)
                .options(sep=sep, header=header, **kwargs)
                .load(object_paths)
            )

            return {"status": True, "data": data}

        except Exception as e:
            self.logger.error(e)
            raise CSVReadError(path=object_paths[0]) from e

    def read_datahub_config(self) -> dict:
        table: str = self.datahub_table
        key: str = f"{self.subtenant}-{self.pipeline}"
        try:
            response = self.ddb_client.Table(table).get_item(Key={"name": key})

            self.logger.info(f"Reading Datahub config {json.dumps(response['Item'])}")

            return {"status": True, "body": response["Item"]}

        except Exception as e:
            self.logger.error(f"Reading Datahub Configuration for name={key} failed: {e}")

            return {"status": False, "error": e}

    def read_hwm_config(self, hwm_id: str, hwm_type: str) -> dict:
        table: str = self.hwm_table
        try:
            response = self.ddb_client.Table(table).get_item(Key={"hwm_id": hwm_id, "hwm_type": hwm_type})

            self.logger.info(f"High Water Mark {json.dumps(response['Item'])}")

            return {"status": True, "body": response["Item"]}

        except Exception as e:
            self.logger.error(f"Reading HWM for hwm_id={hwm_id} failed: {e}")

            return {"status": False, "error": None}

    def read_json(self, bucket: str, key: str, print_content: bool = True) -> dict:
        try:
            self.logger.info(f"Reading file object from s3://{bucket}/{key}")

            object_body = self.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

            data = json.loads(object_body)

            if print_content:
                self.logger.info(f"File data {json.dumps(data)}")
            return {"status": True, "body": data}

        except Exception as e:
            self.logger.error(f"Reading object failed from s3://{bucket}/{key}: {str(e)}")
            return {"status": False, "error": e}

    def read_parquet(self, base_path: str, key: str) -> dict:
        try:
            data: DataFrame = self.spark_session.read.option("basePath", base_path).parquet(
                f"{base_path}/{key}"
            )

            self.logger.info(f"Reading {base_path}/{key}")
            return {"status": True, "data": data}

        except Exception as e:
            self.logger.error(f"Failed to read parquet file {base_path}/{key}: {str(e)}")
            return {"status": False, "data": self.empty_df}

    def read_parquet_files(self, base_path: str, keys: List[str]) -> dict:
        keys = [f"{base_path}/{key}" for key in keys]
        self.logger.info(f"Checking for valid parquet files in {base_path}")

        def path_exists(path):
            sc = self.spark_session.sparkContext
            hpath = sc._jvm.org.apache.hadoop.fs.Path(path)
            fs = hpath.getFileSystem(sc._jsc.hadoopConfiguration())
            return len(fs.globStatus(hpath)) > 0

        valid_keys = [path for path in keys if path_exists(path)]
        self.logger.info(f"Removed {len(keys) - len(valid_keys)} invalid paths")

        try:
            if not valid_keys:
                raise NoNewDataError(table=base_path)

            self.logger.info(f"Reading parquet files from: {valid_keys}")
            data: DataFrame = self.spark_session.read.option("basePath", base_path).parquet(
                *valid_keys
            )
            return {"status": True, "data": data}

        except NoNewDataError as e:
            self.logger.error(f"No valid parquet file paths to read {str(e)}")
            return {"status": False, "data": self.empty_df}

        except Exception as e:
            self.logger.error(f"Failed to read parquet files {str(e)}")
            return {"status": False, "data": self.empty_df}

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
            self.ddb_client.Table(table).put_item(Item=item)

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

    def union_dataframes(self, data: List[DataFrame]) -> DataFrame:
        """Returns a new :class:`DataFrame` containing union of rows in this and another
        :class:`DataFrame`.

        Examples
        --------
        Resolves columns by name (not by position):

        >>> df = unionByName(data=[df1, df2, df3, ...])
        """

        dfs = [df for df in data if not df.rdd.isEmpty()]
        df = self.empty_df

        if len(dfs) == 0:
            df = data[0]
        if len(dfs) == 1:
            df = dfs[0]
        if len(dfs) > 1:
            df = reduce(DataFrame.unionByName, dfs)

        return df

    def s3_sink(
        self,
        data: DataFrame,
        mode: str,
        partitions: List[str],
        path: str,
        schema: list,
        object_per_partition: int = 0,
        force_schema: bool = True
    ) -> dict:
        """Saves the content of the :class:`DataFrame` in Parquet format.

        Parameters
        ----------
        data : DataFrame
            the Spark Dataframe to output.
        mode : str
            specifies the behavior of the save operation when data already exists.
            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.

        partitions : list
            Name of columns as list.
        path : str
            The `s3` path.
        crawler : str
            Name of `crawler` to run on processed data.

        Examples
        --------
        >>> glue_catelog.write_s3_sink(
                data=df,
                partitions=['partitions'],
                s3_path='s3://bucket/prefix/',
                mode='append',
                crawler='crawler-name')
        """
        if force_schema:
            data = self._conform_to_schema(data, schema=schema)

        if data.rdd.isEmpty():
            return {"status": False, "comments": "No new data to write"}

        try:
            self.logger.info(f"Writing data to {path} using S3 Sink")

            if object_per_partition == 0:
                data.write.mode(mode).format("parquet").partitionBy(partitions).option(
                    "path", path
                ).option("partitionOverwriteMode", "dynamic").save()

            else:
                data.repartition(numPartitions=object_per_partition).write.mode(mode).format(
                    "parquet"
                ).partitionBy(partitions).option("path", path).option(
                    "partitionOverwriteMode", "dynamic"
                ).save()

            bucket = path.split('/')[2]
            key = path.split(f'{bucket}/')[1]
            key += '_$folder$'
            self.s3_client.delete_object(Bucket = bucket, Key = key)
            return {"status": True, "comments": "Writing completed to {path} using S3 Sink"}

        except Exception as e:
            self.logger.error(e)
            raise S3SinkWriteError(target_path=path) from e

    def _conform_to_schema(self, data: DataFrame, schema: list) -> DataFrame:
        """
        Conforms input dataframe to provided schema, selecting columns and casting to types
        Called prior to writing data to S3 to ensure all datatypes are as expected.

        Args:
            data (DataFrame): Target dataframe to select/cast types
            schema (dict): Dictionary of column:type read from datahub_config.json targets

        Returns:
            DataFrame: Final dataframe ready to write to s3
        """
        final_schema = []
        for column in schema:
            final_schema.append(F.col(column[0]).cast(column[1]))

        self.logger.info("Final data schema:")
        data = data.select(final_schema)
        data.printSchema()

        return data


class Glue(object):
    def __init__(self, args: List[str]) -> None:
        self.logger = logging.getLogger(__class__.__name__)

        self.logger.info("Parsing Glue Job Arguments")
        self.args: dict = self.parse_glue_arguments(args=args)

        self.logger.info("Creating Spark Session for Glue Job")
        self.spark_session, self.glue_context = self.create_spark_session()

        self.logger.info(f"Initializing Glue Job for {self.args['pipeline']}")
        self.job = self.initialize_glue_job(
            context=self.glue_context, pipeline=self.args["pipeline"], args=self.args
        )

        self.empty_df: DataFrame = self.spark_session.createDataFrame([], StructType([]))

    def dataframe_from_catalog_no_predicate(
        self, database: str, table: str, columns: List[str], **kwargs
    ) -> dict:
        """Read Data from Glue Catalog w/o using Predicate"""

        push_down_predicate: str = ""
        response: dict = self.dataframe_from_catalog(
            database, table, columns, push_down_predicate, **kwargs
        )

        return response

    def dataframe_from_catalog_with_config_predicate(
        self, database: str, table: str, columns: List[str], predicate_config: dict, **kwargs
    ) -> dict:
        """Read Data from Glue Catalog using Predicate"""

        push_down_predicate: str = self.predicate_from_config(config=predicate_config)

        response: dict = self.dataframe_from_catalog(
            database, table, columns, push_down_predicate, **kwargs
        )

        return response

    def dataframe_from_catalog_with_dataframe_predicate(
        self, database: str, table: str, columns: List[str], predicate_data: DataFrame, **kwargs
    ) -> dict():
        """Read Data from Glue Catalog using Predicate"""

        push_down_predicate: str = self.predicate_from_dataframe(data=predicate_data)

        response: dict = self.dataframe_from_catalog(
            database, table, columns, push_down_predicate, **kwargs
        )

        return response

    def dataframe_from_catalog(
        self,
        database: str,
        table: str,
        columns: List[str],
        push_down_predicate: str,
        use_prod_data: bool = False,
        use_spark_read: bool = True,
        **kwargs,
    ) -> dict:
        """Read Data from Glue Catalog using Predicate"""

        if use_prod_data:
            database = "dst_prod_" + database

        self.logger.info(
            f"Reading {database}.{table} for"
            f" {push_down_predicate if push_down_predicate else 'NO PREDICATE'}"
        )

        try:
            if use_spark_read:
                df: DynamicFrame = self.glue_context.create_data_frame.from_catalog(
                    database=database,
                    table_name=table,
                    push_down_predicate=push_down_predicate,
                    additional_options={**kwargs},
                )

                if df.rdd.isEmpty():
                    raise NoNewDataError(table=table)

                df = df.select(columns)

            else:
                dyf: DynamicFrame = self.glue_context.create_dynamic_frame.from_catalog(
                    database=database,
                    table_name=table,
                    push_down_predicate=push_down_predicate,
                    **kwargs,
                ).select_fields(columns)

                df = dyf.toDF()

            if df.rdd.isEmpty():
                raise NoNewDataError(table=table)

            return {"status": True, "data": df}

        except NoNewDataError as e:
            self.logger.info(e)
            raise

        except Exception as e:
            self.logger.error(e)
            raise GlueTableReadError(
                database=database,
                table=table,
            ) from e

    @property
    def session(self) -> SparkSession:
        return self.spark_session

    @property
    def context(self) -> GlueContext:
        return self.glue_context

    @property
    def config(self) -> dict:
        return self.args

    @staticmethod
    def parse_glue_arguments(args: List[str]) -> dict:
        config: dict = getResolvedOptions(args=sys.argv, options=args)
        config: dict = {key.lower(): value for key, value in config.items()}

        return config

    @staticmethod
    def create_spark_session() -> Tuple[SparkSession, GlueContext]:
        spark: SparkContext = SparkContext.getOrCreate()
        glue_context: GlueContext = GlueContext(spark)

        spark_session = glue_context.spark_session
        spark_session.conf.set("spark.sql.crossJoin.enabled", "true")
        spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        return spark_session, glue_context

    @staticmethod
    def initialize_glue_job(context: GlueContext, pipeline: str, args: dict) -> Job:
        job = Job(glue_context=context)
        job.init(job_name=pipeline, args=args)

        return job

    @staticmethod
    def predicate_from_dataframe(data: DataFrame) -> str:
        try:
            if len(data.columns) == 1:
                # only one column - use IN
                column_name: str = data.columns[0]
                rows: List[Row] = data.collect()
                values = [f"'{row[0]}'" for row in rows]

                predicate = f"{column_name} IN ({','.join(values)})"

            else:
                rows: List[Row] = data.collect()
                conditions = []
                for row in rows:
                    condition = []
                    for key, value in row.asDict().items():
                        condition.append(f"{key}='{value}'")

                    conditions.append(f'({" and ".join(condition)})')

                predicate = " or ".join(conditions)

            return predicate

        except Exception as e:
            raise (e)

    @staticmethod
    def predicate_from_config(config: dict) -> str:
        """
        >>> {
              predicate_key1: {
                "value": "value",
                "operator": ">",
                "type": "type",
                "format": "format",
                "increment": "0"
              },
             predicate_key2: {
                "value": "value",
                "operator": ">",
                "type": "type",
                "format": "format",
                "increment": "0"
              },
        }

        Args:
            config (dict): _description_

        Returns:
            str: _description_
        """

        predicates: List[str] = []
        for key, details in config.items():
            predicates.append(Glue.parse_predicate(key=key, config=details))

        return " and ".join(predicates)

    @staticmethod
    def parse_predicate(key: str, config: dict) -> str:
        """_summary_

        Args:
            key (str): _description_
            config (dict):
            >>>  {
                    "value": "20210101000000000",
                    "operator": ">",
                    "type": "datetime",
                    "format": "%Y%m%d%H%M%S",
                    "increment": "0"
                }

        Returns:
            str: _description_
        """

        value = config.get("value", "20220524000000000")
        operator = config.get("operator", ">")
        increment = config.get("increment", "0")
        type = config.get("type", "datetime")
        format = config.get("format", "%Y%m%d%H%M%S%f")

        increment = int(increment)
        condition = f"{key} {operator} {value}"

        from_value = value
        to_value = value

        if type == "string":
            increment = 0
            to_value = f"'{value}'"

        if type == "datetime":
            from_datetime = datetime.strptime(value, format)
            to_datetime = from_datetime + timedelta(increment)
            from_value = f"'{value}'"
            to_value = f"'{to_datetime.strftime(format)}'"

        if type == "date":
            from_date = datetime.strptime(value, format)
            to_date = from_date + timedelta(increment)
            from_value = f"'{value}'"
            to_value = f"'{to_date.strftime(format)}'"

        if type == "month":
            # Set start to last day of previous month
            from_date = datetime.strptime(value, format) + relativedelta(months=-1, day=31)
            # when negative increment, move start date back by x months
            if increment <= 0:
                to_date = from_date + relativedelta(months=increment, day=31)
            # read data from end of prev month to end of future month (x + increment)
            elif increment > 0:
                to_date = from_date + relativedelta(months=1 + increment, day=31)

            from_value = f"'{from_date.strftime(format)}'"
            to_value = f"'{to_date.strftime(format)}'"

        if type == "numeric":
            from_value = int(value)
            to_value = int(value) + increment

        if increment <= 0:
            # Move data back and process
            condition = f"{key} {operator} {to_value}"

        if increment > 0:
            # define data range
            inner_limit = f"{key} > {from_value}"
            outer_limit = f"{key} <= {to_value}"
            condition = f"{inner_limit} AND {outer_limit}"

        return condition


class NoNewDataError(Exception):
    """Raised when required table has no new data available in dataframe"""

    def __init__(self, table: str) -> None:
        super().__init__(f"No new data available for {table}")


class CSVReadError(Exception):
    """Raised when job fails to read raw csv files"""

    def __init__(self, path: str) -> None:
        super().__init__(f"Failed to read csv files from {path}")


class EntityNotFoundError(Exception):
    """Raised when AWS entity cannot be found"""

    def __init__(self, entity: str) -> None:
        super().__init__(f"{entity} not found")


class GlueTableReadError(Exception):
    """Raised when glue table read method fails"""

    def __init__(self, database: str, table: str) -> None:
        super().__init__(f"Failed to read {database}/{table}")


class S3SinkWriteError(Exception):
    """Raised s3 sink method fails to write data"""

    def __init__(self, target_path: str) -> None:
        super().__init__(f"Data writing at {target_path} failed")
