from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_iam as iam,
    aws_efs as efs,
    aws_logs as logs,
    aws_sqs as sqs,
    aws_lambda_destinations as destinations,
    aws_cloudwatch as cloudwatch,
)
from typing import Optional
import json


class LambdaManager(Construct):
    """
    Creates and manages all Lambda functions.
    Single construct that handles all Lambda creation and permissions.
    """
    
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config,
        networking,
        storage,
        database,
        efs,
        constants,
        secrets,
    ):
        super().__init__(scope, construct_id)
        
        self.functions: dict[str, _lambda.Function] = {}
        self.config = config
        self.networking = networking
        self.storage = storage
        self.database = database
        self.efs = efs
        self.constants = constants
        self.secrets = secrets
        
        self._pandas_layer = None
        self._neo4j_layer = None
        self._requests_layer = None
        self._pytz_layer = None
        self._async_dlqs: dict[str, sqs.Queue] = {}

        self._health_check_id = constants.HEALTH_CHECK_FUNCTION_ID
        self._cloudwatch_log_check_id = constants.CLOUDWATCH_LOG_CHECK_FUNCTION_ID
        
        from its4us_cdk.configuration.lambdas import LAMBDAS
        
        for config in LAMBDAS:
            lambda_fn = self._create_lambda(config)
            self.functions[config.id] = lambda_fn
        
        self._setup_invoke_permissions()
    
    def _create_lambda(self, config) -> _lambda.Function:
        """Create a single Lambda function."""
        
        stack = Stack.of(self)
        env = {
            "STAGE": self.config.stage_name,
            "AWS_REGION_NAME": stack.region,
            "AWS_ACCOUNT_ID": stack.account,
            "FACTORS_FILE_KEY": self.config.factors_file_key,
        }
        
        if config.needs_neptune:
            env["NEPTUNE_ENDPOINT"] = self.config.neptune_endpoint
            env["NEPTUNE_CLUSTER_ID"] = self.config.neptune_cluster_id
            # Derive Neptune URLs from endpoint
            env["BASE_DB_URL"] = f"https://{self.config.neptune_endpoint}:8182"
            env["QUERY_URL"] = f"bolt://{self.config.neptune_endpoint}:8182"
            env["LOADER_URL"] = f"https://{self.config.neptune_endpoint}:8182/loader"
            # Neptune S3 IAM role ARN
            env["NEPTUNE_S3_ROLE_ARN"] = f"arn:aws:iam::{stack.account}:role/{self.config.neptune_s3_role_name}"
        
        if config.needs_upload_bucket:
            env["UPLOAD_BUCKET"] = self.storage.upload_bucket.bucket_name
            
            env["WAZE_FOLDER"] = self.constants.WAZE_FOLDER
            env["PERFORMANCE_METRICS_FOLDER"] = self.constants.PERFORMANCE_METRICS_FOLDER
            #env["NAVIGATOR_FOLDER"] = self.constants.NAVIGATOR_FOLDER # future migration from legacy bucket

        if config.id == "retrieve-pmd-arcadis":
            env["ARCADIS_API_KEY_SECRET_ID"] = self.secrets.arcadis_api_key.secret_name
            env["ARCADIS_API_URL_ROOT"] = self.config.arcadis_api_url_root

        if config.id == "import-waze":
            env["WAZE_URLS"] = json.dumps(self.config.waze_feed_urls)
        
        if config.needs_public_export_bucket:
            env["PUBLIC_EXPORT_BUCKET"] = self.storage.public_export_bucket.bucket_name
        
        if config.needs_admin_export_bucket:
            env["ADMIN_LOG_BUCKET"] = self.storage.admin_export_bucket.bucket_name

        if config.needs_legacy_navigator_bucket:
            # Legacy aliases for backward compatibility during migration
            env["NAVIGATOR_BUCKET"] = self.storage.navigator_bucket.bucket_name # legacy bucket

        if config.needs_legacy_pmd_bucket:
            # Legacy aliases for backward compatibility during migration
            env["ARCADIS_PMD_BUCKET"] = self.storage.arcadis_pmd_bucket.bucket_name # legacy bucket
        
        layers = []
        if "pandas" in config.layers:
            if not self._pandas_layer:
                from its4us_cdk.configuration.layers import get_pandas_layer
                self._pandas_layer = get_pandas_layer(self, Stack.of(self).region)
            layers.append(self._pandas_layer)
        
        if "neo4j" in config.layers:
            if not self._neo4j_layer:
                from its4us_cdk.configuration.layers import get_neo4j_layer
                self._neo4j_layer = get_neo4j_layer(self, Stack.of(self).region, Stack.of(self).account)
            layers.append(self._neo4j_layer)
        
        if "requests" in config.layers:
            if not self._requests_layer:
                from its4us_cdk.configuration.layers import get_requests_layer
                self._requests_layer = get_requests_layer(self, Stack.of(self).region, Stack.of(self).account)
            layers.append(self._requests_layer)
        
        if "pytz" in config.layers:
            if not self._pytz_layer:
                from its4us_cdk.configuration.layers import get_pytz_layer
                self._pytz_layer = get_pytz_layer(self, Stack.of(self).region, Stack.of(self).account)
            layers.append(self._pytz_layer)

        # create log group for the lambda function
        log_group = logs.LogGroup(
            self, "LambdaFunctionLogGroup-"+f"cdk-{config.id}-{self.config.stage_name}",
            log_group_name=f"/aws/lambda/cdk-{config.id}-{self.config.stage_name}",  # must match function name
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY # ensure deletion
        )
        
        lambda_fn = _lambda.Function(
            self,
            config.id,
            function_name=f"cdk-{config.id}-{self.config.stage_name}",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler=f"{config.handler_file}.{config.handler_function}",
            code=_lambda.Code.from_asset(config.handler_path),
            memory_size=config.memory_mb,
            timeout=Duration.seconds(config.timeout_seconds),
            reserved_concurrent_executions=config.reserved_concurrency,
            environment=env,
            description=config.description,
            layers=layers,
            vpc=self.networking.vpc if config.needs_vpc else None,
            security_groups=[self.networking.sg, self.networking.efs_sg] if config.needs_vpc else None,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ) if config.needs_vpc else None,
            filesystem=_lambda.FileSystem.from_efs_access_point(
                self.efs.efs_access_point,
                "/mnt/fs1" # Lambda sees EFS here
            ) if config.needs_efs else None,
            log_group=log_group,
        )
        
        self._grant_s3_permissions(lambda_fn, config)

        if config.reserved_concurrency is not None:
            dlq = sqs.Queue(
                self,
                f"{config.id.replace('-', '').title()}AsyncDlq",
                retention_period=Duration.days(14),
            )
            self._async_dlqs[config.id] = dlq
            lambda_fn.configure_async_invoke(
                max_event_age=Duration.hours(1),
                retry_attempts=2,
                on_failure=destinations.SqsDestination(dlq),
            )
            cloudwatch.Alarm(
                self,
                f"{config.id.replace('-', '').title()}AsyncDlqAlarm",
                metric=dlq.metric_approximate_number_of_messages_visible(),
                threshold=1,
                evaluation_periods=1,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            )

        if config.id == "retrieve-pmd-arcadis":
            self.secrets.arcadis_api_key.grant_read(lambda_fn)
        
        if config.needs_neptune:
            lambda_fn.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["neptune-db:Connect"],
                    resources=[
                        f"arn:aws:rds:{Stack.of(self).region}:{Stack.of(self).account}:cluster:{self.config.neptune_cluster_id}"
                    ]
                )
            )
        
        if config.needs_efs:
            # allow Lambda to access EFS
            self.efs.file_system.connections.allow_default_port_from(lambda_fn)

        if config.id in (self._health_check_id, self._cloudwatch_log_check_id):
            lambda_fn.add_to_role_policy(
                iam.PolicyStatement(
                    actions=[
                        "codepipeline:PutJobSuccessResult",
                        "codepipeline:PutJobFailureResult",
                    ],
                    resources=["*"],
                )
            )

        # allow check-cloudwatch-log to check logs of Lambda functions
        if config.id == self._cloudwatch_log_check_id:
            lambda_fn.add_to_role_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "logs:DescribeLogGroups",
                        "logs:DescribeLogStreams",
                        "logs:GetLogEvents",
                    ],
                    resources=[
                        f"arn:aws:logs:{Stack.of(self).region}:{Stack.of(self).account}:log-group:*"
                    ]
                )
            )

        return lambda_fn

    def _grant_s3_permissions(self, lambda_fn: _lambda.Function, config) -> None:
        """Grant only the S3 operations required by a Lambda implementation."""
        bucket = self.storage

        if config.id == "base-impedance-calculation-helper":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", self.config.factors_file_key)
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "impedance_calculation/*")
        elif config.id == "bulk-load":
            for prefix in (
                "osm-updated/*",
                "vds_bulk_loader/*",
                "impedance/*",
                "impedance_calculation/*",
                "performance-metrics/*",
            ):
                self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", prefix)
        elif config.id == "bulk-loader-osm":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", "osm-updated.xml")
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "osm-updated/*")
        elif config.id == "calculate-impedance-helper":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", self.config.factors_file_key)
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "impedance_calculation/*")
        elif config.id == "calculate-performance-metrics":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", "performance-metrics/*")
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "performance-metrics/*")
        elif config.id == "aggregate-performance-metrics":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", "performance-metrics/*")
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "performance-metrics/*")
        elif config.id == "import-impedance":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", "*")
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "impedance/*")
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:DeleteObject", "*")
        elif config.id == "import-vds":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "vds_bulk_loader/*")
            self._grant_object(lambda_fn, bucket.navigator_bucket, "s3:GetObject", "vds_data_*")
        elif config.id == "import-waze":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "waze-alerts/*")
        elif config.id == "retrieve-pmd-arcadis":
            if self.config.stage_name != "prod":
                self._grant_object(lambda_fn, bucket.upload_bucket, "s3:PutObject", "performance-metrics/*")
            key = "*" if self.config.stage_name == "prod" else "performance-metrics/*"
            self._grant_object(lambda_fn, bucket.arcadis_pmd_bucket, "s3:PutObject", key)
        elif config.id == "validate-sidewalk-data":
            self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", self.config.factors_file_key)
            for prefix in ("sidewalk/", "sidewalk-data/", "input/", "uploads/"):
                self._grant_object(lambda_fn, bucket.upload_bucket, "s3:GetObject", f"{prefix}*")
            self._grant_list(lambda_fn, bucket.upload_bucket)

        if config.needs_public_export_bucket:
            if config.id in {"dump-database", "export-impedance"}:
                self._grant_object(lambda_fn, bucket.public_export_bucket, "s3:PutObject", "*")
            elif config.id in {"calculate-impedance-helper", "aggregate-performance-metrics"}:
                self._grant_object(lambda_fn, bucket.public_export_bucket, "s3:GetObject", "*")
                self._grant_object(lambda_fn, bucket.public_export_bucket, "s3:PutObject", "*")

        admin_log_writers = {
            "base-impedance-calculation",
            "base-impedance-calculation-helper",
            "calculate-impedance",
            "calculate-impedance-helper",
            "calculate-performance-metrics",
            "dump-database",
            "export-impedance",
            "import-vds",
            "import-waze",
            "import-navigator",
            "retrieve-pmd-arcadis",
            "delete-pmd-arcadis",
            "aggregate-performance-metrics",
            "validate-sidewalk-data",
        }
        if config.id in admin_log_writers:
            self._grant_object(lambda_fn, bucket.admin_export_bucket, "s3:PutObject", "*")
            if config.id == "base-impedance-calculation-helper":
                self._grant_object(lambda_fn, bucket.admin_export_bucket, "s3:GetObject", "*")

        if config.needs_legacy_navigator_bucket and config.id != "import-vds":
            for prefix in ("scheduled_event_*", "unscheduled_event_*", "event_comment_*", "event_property_*"):
                self._grant_object(lambda_fn, bucket.navigator_bucket, "s3:GetObject", prefix)
            if config.id == "import-navigator":
                self._grant_list(lambda_fn, bucket.navigator_bucket)

        if config.needs_legacy_pmd_bucket:
            if config.id == "delete-pmd-arcadis":
                key = "*" if self.config.stage_name == "prod" else "performance-metrics/*"
                self._grant_object(lambda_fn, bucket.arcadis_pmd_bucket, "s3:DeleteObject", key)
            else:
                key = "*" if self.config.stage_name == "prod" else "performance-metrics/*"
                self._grant_object(lambda_fn, bucket.arcadis_pmd_bucket, "s3:GetObject", key)

        if config.id == "calculate-performance-metrics" and config.needs_legacy_pmd_bucket:
            key = "*" if self.config.stage_name == "prod" else "performance-metrics/*"
            self._grant_object(lambda_fn, bucket.arcadis_pmd_bucket, "s3:GetObject", key)

    @staticmethod
    def _grant_object(lambda_fn, bucket, action: str, key: str) -> None:
        resource = f"{bucket.bucket_arn}/*" if key == "*" else f"{bucket.bucket_arn}/{key}"
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(actions=[action], resources=[resource])
        )

    @staticmethod
    def _grant_list(lambda_fn, bucket) -> None:
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(actions=["s3:ListBucket"], resources=[bucket.bucket_arn])
        )
    
    def _setup_invoke_permissions(self):
        """Grant invoke permissions between Lambdas."""
        from its4us_cdk.configuration.lambdas import LAMBDAS
        
        for config in LAMBDAS:

            # grant lambda function to invoke a specific function
            if config.invokes:
                caller = self.functions[config.id]
                
                for target_id in config.invokes:
                    target = self.functions.get(target_id)
                    
                    if target:
                        target.grant_invoke(caller)
                        
                        env_var = f"{target_id.upper().replace('-', '_')}_ARN"
                        caller.add_environment(env_var, target.function_arn)

            target = self.functions[config.id]

            # grant check-function-health and check-cloudwatch-log to invoke all lambda functions
            if config.id != self._health_check_id and config.id != self._cloudwatch_log_check_id:
                health_check_caller = self.functions[self._health_check_id]
                target.grant_invoke(health_check_caller)

                cloudwatch_log_caller = self.functions[self._cloudwatch_log_check_id]
                target.grant_invoke(cloudwatch_log_caller)

    
    def get(self, lambda_id: str) -> Optional[_lambda.Function]:
        """Get a Lambda by ID."""
        return self.functions.get(lambda_id)
