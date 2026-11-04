from dataclasses import dataclass

@dataclass
class CodePipeline:
    """Configuration for a codepipeline for each Lambda function."""
    method: str
    lambda_id: str
    handler_path: str
    pipeline_name: str
    parameters: dict | None = None
    

CODE_PIPELINES = [

    # base-impedance-calculation
    CodePipeline(
        method="POST",
        lambda_id="base-impedance-calculation",
        handler_path="aws_lambda/base_impedance_calculation",
        pipeline_name="BaseImpedanceCalculatePipeline",
    ),

    # base-impedance-calculation-helper
    CodePipeline(
        method="POST",
        lambda_id="base-impedance-calculation-helper",
        handler_path="aws_lambda/base_impedance_calculation_helper",
        pipeline_name="BaseImpedanceCalculateHelperPipeline",
    ),

    # calculate-impedance
    CodePipeline(
        method="POST",
        lambda_id="calculate-impedance",
        handler_path="aws_lambda/calculate_impedance",
        pipeline_name="ImpedanceCalculatePipeline",
        parameters={
            "body": {"id": "34.0N84.4W", # a random grid to test
                     "testWaze": False}, 
        }
    ),

    # calculate-impedance-helper
    CodePipeline(
        method="POST",
        lambda_id="calculate-impedance-helper",
        handler_path="aws_lambda/calculate_impedance_helper",
        pipeline_name="ImpedanceCalculateHelperPipeline",
        parameters={
            "body": {"id": "34.0N84.4W", # a random grid to test
                     "testWaze": False}, 
        }
    ),

    # TODO: add bulk-load after the method solidify
    # TODO: add check-bulk-load after the method solidify
    # TODO: add validate-sidewalk-data after the method solidify

    # dump-database
    CodePipeline(
        method="POST",
        lambda_id="dump-database",
        handler_path="aws_lambda/dump_database",
        pipeline_name="DumpDatabasePipeline",
        parameters={
            "body": {"id": "34.0N84.4W"}, # a random grid to test
        }
    ),

    # export-impedance
    CodePipeline(
        method="POST",
        lambda_id="export-impedance",
        handler_path="aws_lambda/export_impedance",
        pipeline_name="ExportImpedancePipeline",
        parameters={
            "body": {"id": "34.0N84.4W"}, # a random grid to test
        }
    ),

    # TODO: add import-impedance after the method solidify

    # import-vds
    CodePipeline(
        method="POST",
        lambda_id="import-vds",
        handler_path="aws_lambda/import_vds",
        pipeline_name="ImportVDSPipeline",
    ),

    # retrieve-pmd-arcadis
    CodePipeline(
        method="POST",
        lambda_id="retrieve-pmd-arcadis",
        handler_path="aws_lambda/retrieve_pmd_arcadis",
        pipeline_name="RetrievePMDArcadisPipeline",
    ),

    # calculate-performance-metrics
    CodePipeline(
        method="POST",
        lambda_id="calculate-performance-metrics",
        handler_path="aws_lambda/calculate_performance_metrics",
        pipeline_name="PMDCalculatePipeline",
    ),

    # aggregate-performance-metrics
    CodePipeline(
        method="POST",
        lambda_id="aggregate-performance-metrics",
        handler_path="aws_lambda/aggregate_performance_metrics",
        pipeline_name="PMDAggregatePipeline",
    ),

    # delete-pmd-arcadis
    CodePipeline(
        method="DELETE",
        lambda_id="delete-pmd-arcadis",
        handler_path="aws_lambda/delete_pmd_arcadis",
        pipeline_name="DeletePMDArcadisPipeline",
    ),

    # import-waze
    CodePipeline(
        method="POST",
        lambda_id="import-waze",
        handler_path="aws_lambda/import_waze",
        pipeline_name="WazePipeline",
        parameters={
            "body": {"id": "34.0N84.4W"}, # a random grid to test
        }
    ),

    # import-navigator
    CodePipeline(
        method="POST",
        lambda_id="import-navigator",
        handler_path="aws_lambda/import_navigator",
        pipeline_name="NavigatorPipeline",
        parameters={
            "body": {"id": "34.0N84.4W"}, # a random grid to test
        }
    ),
    
]