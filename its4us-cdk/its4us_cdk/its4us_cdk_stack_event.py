# its4us_cdk/its4us_cdk_stack_event.py
from constructs import Construct

from aws_cdk import (
    Stack,
)

from its4us_cdk.constructs.scheduled_jobs import ScheduledJobsConstruct


class EventStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, stage: str, 
                 lambda_gateway_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # access resources from lambda stack
        lambda_manager = lambda_gateway_stack.lambda_manager

        # 8. Scheduled jobs
        scheduled_jobs = ScheduledJobsConstruct(
            self,
            "ScheduledJobs",
            stage=stage,
            lambda_manager=lambda_manager,
        )