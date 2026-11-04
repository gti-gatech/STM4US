from constructs import Construct
from aws_cdk import (
    Stack,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    Duration,
)

class CodePipelineConstruct(Construct):
    """
    Import an existing CodeCommit repository and creates
    CodePipeline for CI/CD deployment.
    """
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config,
        lambda_manager,
        codecommit_repo: str,
        constants,
    ):
        super().__init__(scope, construct_id)

        # import source repository
        repo = codecommit.Repository.from_repository_name(
            self, "CDKRepository",
            repository_name=codecommit_repo
        )

        # === SOURCE STAGE: SAME FOR ALL LAMBDA FUNCTIONS ===
        source_output = codepipeline.Artifact("SourceOutput")
        
        source_action = codepipeline_actions.CodeCommitSourceAction(
            action_name="Source",
            repository=repo,
            branch=config.stage_name, # CodeCommit <stage> branch: dev and stage
            output=source_output,
            trigger=codepipeline_actions.CodeCommitTrigger.EVENTS  # Auto-trigger
        )

        # find base-impedance-calculation-helper Lambda function helper ARN
        target_id = constants.BASE_IMPEDANCE_CALCULATION_HELPER_ID
        target = lambda_manager.get(target_id)
        BASE_IMPEDANCE_CALCULATION_HELPER_ARN = target.function_arn

        # find calculate-impedance-helper Lambda function helper ARN
        target_id = constants.CALCULATE_IMPEDANCE_HELPER_ID
        target = lambda_manager.get(target_id)
        CALCULATE_IMPEDANCE_HELPER_ARN = target.function_arn

        # retrieve health check function
        health_check_fn = lambda_manager.get(constants.HEALTH_CHECK_FUNCTION_ID)

        # retrieve cloudwatch log check function
        cloudwatch_log_check_fn = lambda_manager.get(constants.CLOUDWATCH_LOG_CHECK_FUNCTION_ID)

        from its4us_cdk.configuration.codepipeline import CODE_PIPELINES

        for pipeline in CODE_PIPELINES:
            lambda_fn = lambda_manager.get(pipeline.lambda_id)
            if not lambda_fn:
                print(f"Warning: Lambda {pipeline.lambda_id} not found for code pipeline")
                continue
        
            # create a pipeline for the lambda function; update if already exist
            pipeline_stages = codepipeline.Pipeline(
                self, pipeline.pipeline_name+config.stage_name.capitalize(),
                pipeline_name=pipeline.pipeline_name+config.stage_name.capitalize(),
                restart_execution_on_update=True,
            )

            # add source stage to the pipeline
            pipeline_stages.add_stage(
                stage_name="Source",
                actions=[source_action]
            )

            # define parameters needed for unit tests
            environment_variables={
                "METHOD": codebuild.BuildEnvironmentVariable(
                    value=pipeline.method,
                ),
                "QUERY_URL": codebuild.BuildEnvironmentVariable(
                    value=config.query_url,
                ),
            }

            if pipeline.parameters:
                datasetid = pipeline.parameters.get("body", {}).get("id")
                datasetid_dict = {"DATASETID": codebuild.BuildEnvironmentVariable(
                    value=datasetid,
                )}

                environment_variables = {**environment_variables, **datasetid_dict}

            # === BUILD STAGE ===
            build_project = codebuild.PipelineProject(
                self, pipeline.pipeline_name+"Build",
                environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                    compute_type=codebuild.ComputeType.MEDIUM,
                ),
                environment_variables=environment_variables,
                build_spec=codebuild.BuildSpec.from_object({
                    "version": "0.2",
                    "phases": {
                        "install": {
                            "runtime-versions": {
                                "python": "3.14",
                            },
                            "commands": [
                                f"pip install -r its4us-cdk/{pipeline.handler_path}/requirements.txt -t .",
                            ]
                        },
                        "pre_build": {
                            "commands": [
                                f"echo Running {pipeline.lambda_id} unit tests to test the lambda function logic ....",
                                f"""
                                    if [ -d "its4us-cdk/{pipeline.handler_path}/tests" ] && find "its4us-cdk/{pipeline.handler_path}/tests" -type f -name "test_*.py" | grep -q .; then
                                      python -m pytest "its4us-cdk/{pipeline.handler_path}/tests/"
                                    else
                                      echo "No test files found, skipping running unit tests"
                                    fi
                                    """
                                f"echo Done running unit tests: {pipeline.lambda_id}"
                            ]
                        },
                        "build": {
                            "commands": [
                            ]
                        },
                        "post_build": {
                            "commands": [
                            ]
                        }
                    },
                    "artifacts": {
                        "files": [f"its4us-cdk/{pipeline.handler_path}/**/*"]
                    },
                    "reports": {
                        "pytest_reports": {
                            "files": [f"its4us-cdk/{pipeline.handler_path}/test-results.xml"],
                            "file-format": "JUNITXML"
                        }
                    }
                }),
                timeout=Duration.minutes(30)
            )
            
            build_output = codepipeline.Artifact("BuildOutput")
            
            build_action = codepipeline_actions.CodeBuildAction(
                action_name="Build",
                project=build_project,
                input=source_output,
                outputs=[build_output]
            )
            
            pipeline_stages.add_stage(
                stage_name="Build",
                actions=[build_action]
            )

            # ===== TEST/INVOKE STAGE (After `cdk deploy`) =====
            # ASSUME DEPLOYMENT OF LAMBDA FUNCTIONS ARE DONE ALEADY BY CDK

            # define user parameters needed
            user_parameters={
                # TODO: add more parameters for other Lambda functions
                "stageVariables": {
                    "ENV": config.stage_name,
                    "UPLOAD_BUCKET": config.s3_upload_bucket_name,
                    "PUBLIC_EXPORT_BUCKET": config.s3_public_export_bucket_name,
                    "ADMIN_LOG_BUCKET": config.s3_admin_export_bucket_name,
                    "NAVIGATOR_BUCKET": config.s3_navigator_bucket_name,
                    "ARCADIS_PMD_BUCKET": config.s3_arcadis_pmd_bucket_name,
                    "PERFORMANCE_METRICS_FOLDER": constants.PERFORMANCE_METRICS_FOLDER,
                    "WAZE_FOLDER": constants.WAZE_FOLDER,
                    "QUERY_URL": config.query_url,
                    "LOADER_URL": config.loader_url,
                    "BASE_IMPEDANCE_CALCULATION_HELPER_ARN": BASE_IMPEDANCE_CALCULATION_HELPER_ARN,
                    "CALCULATE_IMPEDANCE_HELPER_ARN": CALCULATE_IMPEDANCE_HELPER_ARN,
                },
                "httpMethod": pipeline.method,
                "pipelineName": pipeline.pipeline_name+config.stage_name.capitalize(),
            }

            if pipeline.parameters:
                user_parameters = {**user_parameters, **pipeline.parameters}
        
            # add test stage with health check
            health_check_action = codepipeline_actions.LambdaInvokeAction(
                action_name="TestFunctionExistence",
                lambda_=health_check_fn,
                user_parameters={
                    "function_name": lambda_fn.function_name
                },
                run_order=1,
            )

            # add test stage with cloudwatch log check
            cloudwatch_log_check_action = codepipeline_actions.LambdaInvokeAction(
                action_name="TestCloudwatchLogExistence",
                lambda_=cloudwatch_log_check_fn,
                user_parameters={
                    "function_name": lambda_fn.function_name
                },
                run_order=2,
            )

            # add test stage with end-to-end test
            e2e_test_action = codepipeline_actions.LambdaInvokeAction(
                action_name="TestDeployedLambda",
                lambda_=lambda_fn,
                user_parameters=user_parameters,
                run_order=3,
            )

            pipeline_stages.add_stage(
                stage_name="Test",
                actions=[health_check_action, e2e_test_action, cloudwatch_log_check_action]
            )
