import pytest
from build_dataset.transform import Transform
from chispa.dataframe_comparer import DataFramesNotEqualError, assert_df_equality


def compare_two_dataframes(transformed, expected):
    """Prints dataframes and raises AssertionError if
    transformed DNE expected

    Args:
        transformed DataFrame: transformed output
        exprected DataFrame: expected output

    Returns:
        None
    """
    print("\n======= Transformed =======\n")
    transformed.show(truncate=True)
    print("\n======= Expected =======\n")
    expected.show(truncate=True)
    raise AssertionError("Actual and expected dataframes are different")

def setup_helper(input_csv, input_filename, utilities, schemabuilder, spark, s3_client):
    """Creates input dataframe and key path

    Args:
        input_csv string: path to input data .csv
        input_filename string: input schema .json
        utilities: Library to help with testing
        schemabuilder: Library to help with testing
        spark: Pyspark simulator to run transformations
        s3_client: Mock S3 client

    Returns:
        input_df DataFrame: input dataframe
        key string: path to input_csv file in s3
    """
    #Extract filename and key for s3
    filename = input_csv.split('/')[-1]
    key = f"custom/main/mobconv/pdi/business_date=20240223/{filename}"

    #Read schema and csv files
    schema_json, col_names = utilities.read_schema_column_json(input_filename)
    schema = schemabuilder.create_schema(schema_json)
    input_csv = utilities.get_file_path(input_csv)

    #Populate mock s3 path
    with open(input_csv) as f:
        s = f.read()
    items = [
        {  #Item to test
            "Key": key,
            "Body": s,
        }
    ]
    utilities.fill_bucket(s3_client, "WS-00-transform-bucket", items)

    #Create input dataframe
    input_df = spark.read.schema(schema).option("header", "true").csv(input_csv)
    return input_df, key

def output_helper(output_csv, output_filename, utilities, schemabuilder, spark):
    """Creates output DataFrame.

    Args:
        output_csv string: path to output test data .csv
        output_filename string: output schema .json
        utilities: Library to help with testing
        schemabuilder: Library to help with testing
        spark: Pyspark simulator to run transformations

    Returns:
        output_df DataFrame: expected dataframe output
    """
    schema_json, col_names = utilities.read_schema_column_json(output_filename)
    schema = schemabuilder.create_schema(schema_json)
    output_csv = utilities.get_file_path(output_csv)
    output_df = spark.read.schema(schema).option("header", "true").csv(output_csv)
    return output_df

def transform_helper(input_df, key, glue, utils, s3_client, ddb_resource, glue_client):
    """Runs the build_dataset.Transform.raw_file transformation function.

    Args:
        input_df DataFrame: needed to set mock value
        key string: Location reading from
        glue: Mocked glue class common.py
        utils: Mocked utils class common.py
        s3_client: Mocked S3 client
        ddb_resource: Mocked DDB resource
        glue_client: Mocked GLUE client

    Returns:
        transformed_df DataFrame: Transformed dataframe
    """

    #Mock the return value of the read_csv_files of the utils class
    utils.read_csv_files.return_value = {'status': True, 'data': input_df}

    #Create build_dataset.Transform object
    transform_object = Transform(
        s3_client = s3_client,
        ddb_client = ddb_resource,
        glue_client = glue_client,
        glue = glue,
        utils = utils
    )

    #Run transformation function
    transformed_df = transform_object.raw_file(key = key)['data']
    return transformed_df

def compare_expected_actual_incremental_dataframes(
        spark,
        utilities,
        schemabuilder,
        s3_client,
        ddb_resource,
        glue_client,
        glue,
        utils,
        input_filename,
        input_csv,
        output_filename,
        output_csv
    ):
    """Tests build_dataset.Transform.raw_file() to compare transformed df with
    expected output

    Args:
        spark: Imitates pyspark to do transformations
        utilities: Library to call common functions
        schemabuilder: Library to build schema
        s3_client: Mock s3 client
        ddb_resource: Mock ddb resource
        glue_client: Mock glue client
        glue: Mock glue class
        utils: Mock utils class
        input_filename string: input schema
        input_csv string: input csv
        output_filename string: output schema
        output_csv string: output csv

    Returns:
        None
    """
    #Setup & Input
    input_df, key = setup_helper(input_csv, input_filename, utilities, schemabuilder, spark, s3_client)  # noqa: E501

    #Output
    output_df = output_helper(output_csv, output_filename, utilities, schemabuilder, spark)  # noqa: E501

    #Transform function
    transformed_df = transform_helper(input_df, key, glue, utils, s3_client, ddb_resource, glue_client)  # noqa: E501

    #Drop columns that cannot be tested properly
    transformed_df = transformed_df.drop(*['creation_datetime', 'datahub_ts'])
    output_df = output_df.drop(*['creation_datetime', 'datahub_ts'])

    #Assertion. Compares expected vs actual dataframe
    try:
        assert_df_equality(
            output_df,
            transformed_df,
            ignore_column_order=True,
            ignore_row_order=True,
            ignore_nullable=True,
        )
    except DataFramesNotEqualError:
        compare_two_dataframes(output_df, transformed_df)

def test_transformations(
        spark,
        utilities,
        schemabuilder,
        s3_client,
        s3_bucket_transform,
        ddb_resource,
        glue_client,
        glue,
        utils
    ):
    """Compare transformed dataframe and expected dataframe for all file types
    in PDI_INC dataset using build_dataset.Transform.raw_file()

    Args:
        spark: Pyspark simulator for transformations
        utilities: Library to help with testing
        schemabuilder: Library to help with testing
        s3_client: Mock S3 client
        s3_bucket_transform string: Path to place input_csv files
        ddb_resource: Mock DDB resource
        glue_client: Mock GLUE client
        glue: Mock glue class common.py
        utils: Mock utils class common.py

    Returns:
        None
    """
    #Datasets have more that 25 fields
    spark.conf.set("spark.sql.debug.maxToStringFields", 100)

    #Datasets to test
    filenames = [
        'gl_accounts_ampm',
        'gl_account_majors_ampm',
        'gl_account_types_ampm',
        'item_brands_ampm',
        'item_attributes_ampm',
        'item_attribute_dates_ampm',
        'item_attrib_valueitemdetails_ampm',
        'item_attrib_valuegroups_ampm',
        'item_group_levels_ampm',
        'item_group_membership_ampm',
        'item_groups_ampm',
        'item_manufacturers_ampm',
        'item_packages_ampm',
        'item_sizes_ampm',
        'item_standard_info_ampm',
        'profit_centers_ampm',
        'retail_packages_ampm',
        'sites_ampm',
        'vendor_costzonesites_ampm',
        'paperwork_batches_ampm',
        'vendor_costs_ampm',
        'vendor_item_attributes_ampm',
        'vendors_ampm'
    ]

    #Test each dataset individually
    for filename in filenames:

        #Specify filepaths for each dataset
        input_filename = f'input_schemas/{filename}.json'
        output_filename = f'output_schemas/{filename}.json'
        input_csv = f'input_files/20240223{filename}.csv'
        output_csv = f'expected_output_files/{filename}.csv'

        #Run test
        compare_expected_actual_incremental_dataframes(
            spark = spark,
            utilities = utilities,
            schemabuilder = schemabuilder,
            s3_client = s3_client,
            ddb_resource = ddb_resource,
            glue_client = glue_client,
            glue = glue,
            utils = utils,
            input_filename = input_filename,
            input_csv = input_csv,
            output_filename = output_filename,
            output_csv = output_csv
        )
