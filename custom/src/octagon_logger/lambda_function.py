import logging
import os
from datalake_library import octagon
from datalake_library.octagon import peh
from custom_json_logger import LambdaJsonLogFilter

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SUBTENANT_ID = os.environ.get("SUBTENANT_ID")
PIPELINE_NAME = os.environ.get("PIPELINE_NAME")


def lambda_handler(event, context):
    lambda_log = LambdaJsonLogFilter.pre_init(context).set_logger(logger)
    execution_id = event.get("peh_id")
    lambda_log.reset(subtenant=SUBTENANT_ID, pipeline=PIPELINE_NAME, peh_id=execution_id)
    lambda_log.info(event)
    ENV = os.environ['ENV']
    PEH_ID = event.get("peh_id")
    LOG_TYPE = event.get("log-type")
    LOG_COMPONENT = event.get("log-component")
    LOG_MSG = event.get("log-msg")

    OCTAGON = octagon.OctagonClient().with_run_lambda(True).with_configuration_instance(ENV).build()

    peh.PipelineExecutionHistoryAPI(OCTAGON).retrieve_pipeline_execution(PEH_ID)

    if LOG_TYPE == "status":
        OCTAGON.update_pipeline_execution(component=LOG_COMPONENT, status=LOG_MSG)

    if LOG_TYPE == "pipeline_failed":
        lambda_log.info(f"{LOG_COMPONENT} failed")
        # Should make sure LOG_MSG is limited size
        # (DynamoDB max attribute size is )
        OCTAGON.end_pipeline_execution_failed(component=LOG_COMPONENT, issue_comment=LOG_MSG)
        lambda_log.info("Completed writing failure info to Octagon")

    if LOG_TYPE == "pipeline_success":
        lambda_log.info("Pipeline succeeded")
        # Should make sure LOG_MSG is limited size
        # (DynamoDB max attribute size is )
        OCTAGON.end_pipeline_execution_success()
        lambda_log.info("Completed writing success info to Octagon")

    lambda_log.info("Octagon Logging Complete")
