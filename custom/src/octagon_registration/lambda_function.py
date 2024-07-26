import logging
import os

from datalake_library import octagon

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context) -> dict:
    """Register Pipeline to Octagon Clint
       Start Pipeline Execution

    Args:
        event (dict): None
        context (object): Lambda function

    Returns:
        dict:
        >>> {
                "peh_id": "<peh_id>"
            }
    """
    stage: str = "standalone"
    subtenant_id: str = os.environ["SUBTENANT_ID"]
    pipeline_name: str = os.environ["PIPELINE_NAME"]
    env: str = os.environ["ENV"]
    logger.info(f"Event input: {event}")

    PEH_ID = event.get("peh_id")
    OCTAGON = octagon.OctagonClient().with_run_lambda(True).with_configuration_instance(env).build()
    # Register Pipeline to Octagon
    OCTAGON.register_custom_pipeline(
        parent_pipeline=pipeline_name,
        child_pipeline=pipeline_name,
        subtenant_id=subtenant_id,
        stage=stage,
    )
    if PEH_ID is None:
        logger.info("No peh_id in input, creating new Octagon pipeline execution")
        # Start Pipeline execution
        PEH_ID: str = OCTAGON.start_pipeline_execution(
            pipeline_name=f"{subtenant_id}-main-{pipeline_name}-standalone"
        )
    else:
        logger.info("peh_id provided as input, skipping Octagon pipeline registration")

    return {"peh_id": PEH_ID}
