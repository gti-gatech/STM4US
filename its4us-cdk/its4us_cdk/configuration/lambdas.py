from dataclasses import dataclass, field


@dataclass
class LambdaConfig:
    """Configuration for a Lambda function."""
    id: str
    handler_path: str
    handler_file: str = "lambda_function"
    handler_function: str = "lambda_handler"
    description: str = ""
    
    memory_mb: int = 512
    timeout_seconds: int = 30
    needs_vpc: bool = True
    needs_neptune: bool = True
    needs_upload_bucket: bool = False
    needs_public_export_bucket: bool = False
    needs_admin_export_bucket: bool = True
    needs_legacy_navigator_bucket: bool = False
    needs_legacy_pmd_bucket: bool = False
    needs_efs: bool = False
    reserved_concurrency: int | None = None
    
    invokes: list[str] = field(default_factory=list)
    layers: list[str] = field(default_factory=list)


LAMBDAS = [
    LambdaConfig(
        id="base-impedance-calculation",
        handler_path="aws_lambda/base_impedance_calculation",
        description="Orchestrator that generates task IDs and invokes helper Lambda for base impedance calculations",
        needs_vpc=False,
        needs_neptune=False,
        needs_admin_export_bucket=True,
        invokes=["base-impedance-calculation-helper"],
        reserved_concurrency=1,
        memory_mb=256,
        timeout_seconds=30,
    ),
    
    LambdaConfig(
        id="base-impedance-calculation-helper",
        handler_path="aws_lambda/base_impedance_calculation_helper",
        description="Worker Lambda that calculates base impedance using factors.csv",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=True,
        needs_admin_export_bucket=True,
        layers=["pandas", "neo4j"],
        memory_mb=2048,
        timeout_seconds=600,
    ),
    
    LambdaConfig(
        id="bulk-load",
        handler_path="aws_lambda/bulk_load",
        description="Initiates Neptune bulk load from S3 CSV files",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=True,
        layers=["requests"],
        memory_mb=256,
        timeout_seconds=60,
    ),
    
    LambdaConfig(
        id="check-bulk-load",
        handler_path="aws_lambda/check_bulk_load",
        description="Checks status of Neptune bulk load jobs",
        needs_vpc=True,
        needs_neptune=True,
        memory_mb=256,
        timeout_seconds=30,
    ),
    
    LambdaConfig(
        id="bulk-loader-osm",
        handler_path="aws_lambda/bulk_loader_osm",
        description="Processes OSM XML data into Neptune-compatible CSVs",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=True,
        layers=["requests"],
        memory_mb=1024,
        timeout_seconds=300,
    ),
    
    LambdaConfig(
        id="calculate-impedance",
        handler_path="aws_lambda/calculate_impedance",
        description="Async coordinator for impedance calculation",
        invokes=["calculate-impedance-helper"],
        reserved_concurrency=1,
        needs_admin_export_bucket=True,
        memory_mb=256,
        timeout_seconds=30,
    ),
    
    LambdaConfig(
        id="calculate-impedance-helper",
        handler_path="aws_lambda/calculate_impedance_helper",
        description="Worker: Calculates updated impedance based on real-time conditions",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=True,
        needs_public_export_bucket=True,
        needs_admin_export_bucket=True,
        layers=["pandas", "neo4j"],
        reserved_concurrency=1,
        memory_mb=1024,
        timeout_seconds=300,
    ),
    
    LambdaConfig(
        id="calculate-performance-metrics",
        handler_path="aws_lambda/calculate_performance_metrics",
        description="Computes daily STM performance metrics from Arcadis data",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=True,
        needs_public_export_bucket=True,
        needs_legacy_pmd_bucket=True,
        needs_efs=True,
        layers=["neo4j", "pytz"],
        memory_mb=2048,
        timeout_seconds=600,
    ),

    LambdaConfig(
        id="aggregate-performance-metrics",
        handler_path="aws_lambda/aggregate_performance_metrics",
        description="Aggregates daily STM performance metrics",
        needs_vpc=False,
        needs_neptune=False,
        needs_upload_bucket=True,
        needs_public_export_bucket=True,
        layers=["pytz"],
        memory_mb=256,
        timeout_seconds=30,
    ),
    
    LambdaConfig(
        id="dump-database",
        handler_path="aws_lambda/dump_database",
        description="Exports sidewalk and impedance data from Neptune to CSV files",
        needs_vpc=True,
        needs_neptune=True,
        needs_public_export_bucket=True,
        layers=["pandas", "neo4j"],
        memory_mb=1024,
        timeout_seconds=300,
    ),
    
    LambdaConfig(
        id="export-impedance",
        handler_path="aws_lambda/export_impedance",
        description="Exports impedance links within study area to CSV",
        needs_vpc=True,
        needs_neptune=True,
        needs_public_export_bucket=True,
        layers=["neo4j"],
        memory_mb=512,
        timeout_seconds=120,
    ),
    
    LambdaConfig(
        id="import-impedance",
        handler_path="aws_lambda/import_impedance",
        description="Bulk loads impedance CSV files to Neptune",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=True,
        layers=["requests"],
        memory_mb=512,
        timeout_seconds=120,
    ),
    
    LambdaConfig(
        id="import-navigator",
        handler_path="aws_lambda/import_navigator",
        description="Ingests NaviGAtor scheduled/unscheduled events from S3 to Neptune",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=False,
        needs_legacy_navigator_bucket=True,
        needs_efs=True,
        layers=["pandas", "neo4j", "pytz"],
        reserved_concurrency=15,
        memory_mb=1024,
        timeout_seconds=300,
    ),
    
    LambdaConfig(
        id="import-vds",
        handler_path="aws_lambda/import_vds",
        description="Processes VDS data from S3 and loads to Neptune",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=True,
        needs_legacy_navigator_bucket=True,
        layers=["pandas", "requests"],
        reserved_concurrency=2,
        memory_mb=512,
        timeout_seconds=120,
    ),
    
    LambdaConfig(
        id="import-waze",
        handler_path="aws_lambda/import_waze",
        description="Fetches Waze alerts and ingests to Neptune",
        needs_vpc=True,
        needs_neptune=True,
        needs_upload_bucket=True,
        needs_efs=True,
        layers=["requests", "neo4j"],
        reserved_concurrency=15,
        memory_mb=512,
        timeout_seconds=120,
    ),
    
    LambdaConfig(
        id="retrieve-pmd-arcadis",
        handler_path="aws_lambda/retrieve_pmd_arcadis",
        description="Retrieves PMD files from Arcadis API and uploads to S3",
        needs_vpc=False,
        needs_neptune=False,
        needs_upload_bucket=True,
        needs_legacy_pmd_bucket=True,
        layers=["requests", "pytz"],
        memory_mb=512,
        timeout_seconds=300,
    ),

    LambdaConfig(
        id="delete-pmd-arcadis",
        handler_path="aws_lambda/delete_pmd_arcadis",
        description="Deletes PMD files from Arcadis on S3",
        needs_vpc=False,
        needs_neptune=False,
        needs_upload_bucket=True,
        needs_legacy_pmd_bucket=True,
        layers=["pytz"],
        memory_mb=256,
        timeout_seconds=30,
    ),
    
    LambdaConfig(
        id="validate-sidewalk-data",
        handler_path="aws_lambda/validate_sidewalk_data",
        description="Validates sidewalk data files in S3 against factors.csv constraints",
        needs_vpc=False,
        needs_neptune=False,
        needs_upload_bucket=True,
        needs_admin_export_bucket=True,
        layers=["pandas"],
        memory_mb=512,
        timeout_seconds=300,
    ),

    LambdaConfig(
        id="check-function-health",
        handler_path="tests/test_lambda/health_check",
        description="Check health and existence of Lambda functions",
        needs_vpc=False,
        needs_neptune=False,
        reserved_concurrency=2,
        memory_mb=256,
        timeout_seconds=5,
    ),

    LambdaConfig(
        id="check-cloudwatch-log",
        handler_path="tests/test_lambda/cloudwatch_log",
        description="Check existence of cloudwatch logs on Lambda functions",
        needs_vpc=False,
        needs_neptune=False,
        reserved_concurrency=1,
        memory_mb=256,
        timeout_seconds=5,
    ),
]
