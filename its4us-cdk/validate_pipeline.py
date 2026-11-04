import sys
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from colorama import Fore, Style, init

# initialize colorama (needed for Windows)
init(autoreset=True)

class PipelineGuard:
    """Guard to prevent unsafe production deployments"""
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.stage_name = "Test"
        self.codepipeline = boto3.client("codepipeline")
    
    def check_stage_success_recent(self, max_age_hours: int = 24) -> tuple[bool, bool, dict]:
        """Check if the stage deployment succeeded and is recent"""

        succeed = False
        recent = False
        execution_info = dict()

        try:
            response = self.codepipeline.get_pipeline_state(name=self.pipeline_name)
            stage_states = response.get("stageStates", [])
            
            deploy_stage = next(
                (stage for stage in stage_states if stage["stageName"] == self.stage_name),
                None
            )

            # check if pipeline is found
            if not deploy_stage:
                return succeed, recent, execution_info

            # check if the last execution has succeeded and is recent
            latest_execution = deploy_stage.get("latestExecution", {})
            status = latest_execution.get("status")
            execution_id = latest_execution.get("pipelineExecutionId")

            # get lastStatusChange from action state - TestDeployedLambda
            action_states = deploy_stage.get("actionStates", [])

            lambda_action = next((action for action in action_states 
                                  if action.get("actionName") == "TestDeployedLambda"), {})

            latest_action = lambda_action.get("latestExecution", {})
            last_update = latest_action.get("lastStatusChange", None)

            # save the pipeline info and status
            execution_info["status"] = status
            execution_info["execution_id"] = execution_id
            execution_info["last_update"] = last_update
            execution_info["action_states"] = action_states

            #print("Details:", execution_info)
            #print("Latest execution:", latest_execution)
            #print("Action state:", action_states)

            if status.lower() == "succeeded":
                succeed = True

            if not last_update:
                return succeed, recent, execution_info
            
            age = datetime.now(last_update.tzinfo) - last_update

            if age < timedelta(hours=max_age_hours):
                recent = True
            
        except ClientError as error:
            print(f"⚠️  Error checking pipeline: {error.response["Error"]["Code"]}")

        return succeed, recent, execution_info
    
    
    def validate_prod_deployment(self, max_age_hours: int = 24):
        """
        Validate that it's safe to deploy to production.
        
        Args:
            max_age_hours: Maximum age of stage deployment in hours
        
        Raises:
            SystemExit: If validation fails
        """
        print(f"\n🔍 Checking stage env pipeline state for prod deployment ...")
        print(f"   Pipeline: {self.pipeline_name}")
        print(f"   Stage: {self.stage_name}")
        
        # determine if pipeline exists, has succeeded and is a recent run
        succeed, recent, details = self.check_stage_success_recent(max_age_hours)

        # check if pipeline exists
        if not details:
            error_msg = f"⚠️  Stage '{self.stage_name}' not found in pipeline: {self.pipeline_name}"
            action_msg = f"Please ensure the pipeline: {self.pipeline_name} exists.\n"

            print(error_msg, file=sys.stderr)
            print(Style.BRIGHT + Fore.BLUE + action_msg, file=sys.stderr)
            sys.exit(1)

        # check if stage succeeded
        if not succeed:
            status = details.get("status", "Unknown")
            
            error_msg = (
                f"\n❌ Cannot deploy to prod: Stage pipeline has not succeeded.\n"
                f"   Pipeline: {self.pipeline_name}\n"
                f"   Stage: {self.stage_name}\n"
                f"   Current Status: {status}\n"
            )
            
            if details.get("execution_id"):
                error_msg += f"   Execution ID: {details["execution_id"]}\n"
            
            action_msg = "Please ensure the pipeline passes before deploying to production.\n"
            
            print(error_msg, file=sys.stderr)
            print(Style.BRIGHT + Fore.BLUE + action_msg, file=sys.stderr)
            sys.exit(1)
        
        # check if deployment is recent
        if not recent:
            last_update = details.get("last_update")

            if last_update:
                # last update time of the test action was more than max_age_hours
                error_msg = (
                    f"\n⚠️  Warning: Stage deployment is older than {max_age_hours} hours.\n"
                    f"   Last deployment: {last_update}\n"
                )

                action_msg = f"Please run the pipeline: {self.pipeline_name} again to ensure tests pass on latest code.\n"
            
            else:
                # last update time of the test action was not found
                error_msg = (
                    f"\n❌  Error: Last TestDeployedLambda action run was not found.\n"
                    f"   Action states: {details.get("action_states")}\n"
                )

                action_msg = "Please check TestDeployedLambda action exists and run the pipeline again.\n"

            # make this a hard failure
            print(error_msg, file=sys.stderr)
            print(Style.BRIGHT + Fore.BLUE + action_msg, file=sys.stderr)
            sys.exit(1)
        
        print(f"✅ Pipeline validation passed on stage env. Safe to deploy to prod.\n")

