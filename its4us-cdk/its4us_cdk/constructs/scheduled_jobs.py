from constructs import Construct
from aws_cdk import (
    Duration,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_cloudwatch as cloudwatch,
)

class ScheduledJobsConstruct(Construct):
    """
    Creates EventBridge rules for scheduled Lambda jobs.
    Jobs are defined in configuration/scheduled_jobs.py
    """
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        stage: str,
        lambda_manager,
    ):
        super().__init__(scope, construct_id)

        from its4us_cdk.configuration.scheduled_jobs import SCHEDULED_JOBS

        scheduled_dlq = sqs.Queue(
            self,
            f"ScheduledJobsDlq{stage.title()}",
            retention_period=Duration.days(14),
        )

        for job in SCHEDULED_JOBS:
            lambda_fn = lambda_manager.get(job.lambda_id)
            if not lambda_fn:
                print(f"Warning: Lambda {job.lambda_id} not found for scheduled job")
                continue

            if job.fixed_rate:
                schedule=events.Schedule.rate(job.fixed_rate)
            else:
                schedule=events.Schedule.cron(
                    week_day=job.cron_weekday,  
                    hour=job.cron_hour,       
                    minute=job.cron_minute,
                )

            # disable schedulers on the delete jobs
            # NOTE: turn delete-pmd-arcadis job on when it is fully implemented
            if job.name == "delete-waze" or job.name == "delete-navigator" \
                or job.name == "delete-pmd-arcadis":
                enable_scheduler = False
            else:
                enable_scheduler = stage == "prod" # enable scheduler when stage is prod

            rule = events.Rule(
                self,
                f"{job.name}-rule",
                rule_name=f"{job.name}-{stage}",
                schedule=schedule,
                description=job.description,
                enabled=enable_scheduler,
            )

            # Pass job-specific payload (environment variables are already set on Lambda)
            rule.add_target(targets.LambdaFunction(
                lambda_fn,
                event=events.RuleTargetInput.from_object(job.payload) if job.payload else None,
                dead_letter_queue=scheduled_dlq,
                retry_attempts=2,
                max_event_age=Duration.hours(1),
            ))

        cloudwatch.Alarm(
            self,
            f"ScheduledJobsDlqAlarm{stage.title()}",
            metric=scheduled_dlq.metric_approximate_number_of_messages_visible(),
            threshold=1,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
