from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_apigateway as apigw,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as targets,
)

from its4us_cdk.configuration.constants import *


class ApiGatewayConstruct(Construct):
    """
    Creates an API Gateway with Lambda integration and authorizer.
    Routes are defined in configuration/api_routes.py
    """
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config,
        lambda_manager,
        authorizer_function: _lambda.IFunction,
        storage,
    ):
        super().__init__(scope, construct_id)

        self.api = apigw.RestApi(
            self,
            f"Api-{config.stage_name}",
            rest_api_name=config.api_name,
            endpoint_types=[apigw.EndpointType.REGIONAL],
            deploy_options=apigw.StageOptions(
                stage_name=config.stage_name,
                variables={
                    # stageVariables - may not be needed, not currently used
                    "ENV": config.stage_name,
                    "ADMIN_LOG_BUCKET": config.s3_admin_export_bucket_name,
                    "BASE_DB_URL": config.base_db_url,
                    "UPLOAD_BUCKET": config.s3_upload_bucket_name,
                    "PUBLIC_EXPORT_BUCKET": config.s3_public_export_bucket_name,
                    "LOADER_URL": config.loader_url,
                    "QUERY_URL": config.query_url,
                    "PERFORMANCE_METRICS_FOLDER": PERFORMANCE_METRICS_FOLDER,
                    #"TEST_FOLDER": TEST_FOLDER,
                    "WAZE_FOLDER": WAZE_FOLDER,
                },
            ),
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=apigw.Cors.ALL_METHODS,
                allow_headers=[
                    "Content-Type",
                    "Authorization",
                    "X-Amz-Date",
                    "X-Api-Key",
                    "X-Amz-Security-Token",
                ],
                allow_credentials=True,
            ),
        )

        authorizer = apigw.TokenAuthorizer(
            self,
            "Authorizer",
            handler=authorizer_function,
            identity_source=apigw.IdentitySource.header("Authorization"),
            results_cache_ttl=Duration.minutes(0),
        )

        # define request body id model
        request_body_id_model = self.api.add_model(
            "RequestModel",
            content_type="application/json",
            model_name="RequestBodyIdModel",
            schema=apigw.JsonSchema(
                type=apigw.JsonSchemaType.OBJECT,
                properties={
                    "id": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.STRING
                    ),
                },
                required=["id"]  # Required fields
            )
        )

        # create request validator
        validator = self.api.add_request_validator(
            "RequestValidator",
            validate_request_body=True,
            validate_request_parameters=True
        )

        from its4us_cdk.configuration.api_routes import API_ROUTES

        resources = {}

        for route in API_ROUTES:
            lambda_fn = lambda_manager.get(route.lambda_id)
            if not lambda_fn:
                print(f"Warning: Lambda {route.lambda_id} not found for route {route.path}")
                continue

            path_parts = [p for p in route.path.split("/") if p]
            current_resource = self.api.root
            resource_path = ""

            for part in path_parts:
                resource_path += f"/{part}"
                if resource_path not in resources:
                    resources[resource_path] = current_resource.add_resource(part)
                current_resource = resources[resource_path]

            # check if a request body is required
            if route.request_model == "RequestBodyIdModel":
                request_model = {"application/json": request_body_id_model}
            else:
                request_model = None


            current_resource.add_method(
                route.method,
                apigw.LambdaIntegration(lambda_fn),
                authorizer=authorizer if route.requires_auth else None,
                authorization_type=(
                    apigw.AuthorizationType.CUSTOM
                    if route.requires_auth
                    else apigw.AuthorizationType.NONE
                ),
                request_parameters=route.request_parameters,
                request_validator=validator,
                request_models=request_model,
            )

        self._setup_s3_integration(authorizer, storage)

        if config.custom_domain_name and config.certificate_id and config.hosted_zone_id:
            self._setup_custom_domain(config)

    def _setup_s3_integration(self, authorizer, storage):
        """Setup direct S3 integration for file uploads/downloads."""
        
        # Create an IAM role for API Gateway to access S3
        api_role = iam.Role(
            self, "ApiGatewayS3Role",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
        )
        
        # The integrations below expose only PUT for uploads and GET for exports.
        api_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:PutObject"],
            resources=[f"{storage.upload_bucket.bucket_arn}/*"],
        ))
        api_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[
                f"{storage.upload_bucket.bucket_arn}/*",
                f"{storage.public_export_bucket.bucket_arn}/*",
                f"{storage.admin_export_bucket.bucket_arn}/*",
            ],
        ))

        # Create /s3 parent resource
        s3_resource = self.api.root.add_resource("s3")
        
        # Create /s3/{bucket-type}/{filename} endpoints
        self._create_s3_bucket_endpoint(
            parent_resource=s3_resource,
            path_segment="upload",
            bucket=storage.upload_bucket,
            api_role=api_role,
            authorizer=authorizer,
            allow_put=True,
        )

        self._create_s3_bucket_endpoint(
            parent_resource=s3_resource,
            path_segment="export",
            bucket=storage.public_export_bucket,
            api_role=api_role,
            authorizer=authorizer,
            allow_put=False,
        )

        self._create_s3_bucket_endpoint(
            parent_resource=s3_resource,
            path_segment="admin-logs",
            bucket=storage.admin_export_bucket,
            api_role=api_role,
            authorizer=authorizer,
            allow_put=False,
        )

    def _create_s3_bucket_endpoint(self, parent_resource, path_segment: str, bucket, api_role, authorizer, allow_put: bool):
        """
        Create /s3/{path_segment}/{path+} endpoint for a specific bucket.
        Supports nested paths like: /s3/upload/folder1/folder2/file.txt
        
        Args:
            path_segment: URL path segment (e.g., "upload", "export", "admin-logs")
            bucket: The actual S3 bucket object to integrate with
        """
        
        bucket_resource = parent_resource.add_resource(path_segment)
        # Use {proxy+} to capture full path including folders
        path_resource = bucket_resource.add_resource("{proxy+}")

        # GET method
        s3_integration_get = apigw.AwsIntegration(
            service="s3",
            integration_http_method="GET",
            path=f"{bucket.bucket_name}/{{proxy}}",
            options=apigw.IntegrationOptions(
                credentials_role=api_role,
                integration_responses=[
                    apigw.IntegrationResponse(
                        status_code="200",
                        response_parameters={
                            "method.response.header.Content-Type": "integration.response.header.Content-Type",
                            "method.response.header.Content-Length": "integration.response.header.Content-Length",
                        },
                    ),
                    apigw.IntegrationResponse(
                        status_code="404",
                        selection_pattern="404",
                    ),
                ],
                request_parameters={
                    "integration.request.path.proxy": "method.request.path.proxy",
                },
            ),
        )

        path_resource.add_method(
            "GET",
            s3_integration_get,
            authorizer=authorizer,
            authorization_type=apigw.AuthorizationType.CUSTOM,
            request_parameters={
                "method.request.path.proxy": True,
            },
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Content-Type": True,
                        "method.response.header.Content-Length": True,
                    },
                ),
                apigw.MethodResponse(
                    status_code="404",
                ),
            ],
        )

        # PUT method (only for buckets that allow writes, admin only via authorizer)
        if allow_put:
            s3_integration_put = apigw.AwsIntegration(
                service="s3",
                integration_http_method="PUT",
                path=f"{bucket.bucket_name}/{{proxy}}",
                options=apigw.IntegrationOptions(
                    credentials_role=api_role,
                    integration_responses=[
                        apigw.IntegrationResponse(
                            status_code="200",
                        ),
                    ],
                    request_parameters={
                        "integration.request.path.proxy": "method.request.path.proxy",
                        "integration.request.header.Content-Type": "method.request.header.Content-Type",
                    },
                ),
            )

            path_resource.add_method(
                "PUT",
                s3_integration_put,
                authorizer=authorizer,
                authorization_type=apigw.AuthorizationType.CUSTOM,
                request_parameters={
                    "method.request.path.proxy": True,
                    "method.request.header.Content-Type": False,
                },
                method_responses=[
                    apigw.MethodResponse(
                        status_code="200",
                    ),
                ],
            )


    def _setup_custom_domain(self, config):
        """Setup custom domain name with Route53 integration."""
        
        stack = Stack.of(self)
        certificate_arn = f"arn:aws:acm:{stack.region}:{stack.account}:certificate/{config.certificate_id}"
        
        certificate = acm.Certificate.from_certificate_arn(
            self,
            "Certificate",
            certificate_arn=certificate_arn
        )

        domain = apigw.DomainName(
            self,
            "CustomDomain",
            domain_name=config.custom_domain_name,
            certificate=certificate,
            endpoint_type=apigw.EndpointType.REGIONAL,
            security_policy=apigw.SecurityPolicy.TLS_1_2,
        )

        domain.add_base_path_mapping(
            self.api,
            stage=self.api.deployment_stage,
        )

        hosted_zone = route53.HostedZone.from_hosted_zone_attributes(
            self,
            "HostedZone",
            hosted_zone_id=config.hosted_zone_id,
            zone_name=config.hosted_zone_name,
        )

        route53.ARecord(
            self,
            "CustomDomainAliasRecord",
            zone=hosted_zone,
            record_name=config.custom_domain_name,
            target=route53.RecordTarget.from_alias(
                targets.ApiGatewayDomain(domain)
            ),
        )

