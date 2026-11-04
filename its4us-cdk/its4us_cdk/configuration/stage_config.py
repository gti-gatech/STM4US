from dataclasses import dataclass
from typing import Optional

@dataclass
class StageConfig:
    """Configuration for a specific deployment stage."""
    stage_name: str
    vpc_id: str
    sg_id: str
    efs_sg_id: str
    neptune_cluster_id: str
    neptune_cluster_resource_id: str
    neptune_endpoint: str
    s3_upload_bucket_name: str
    s3_admin_export_bucket_name: str
    s3_public_export_bucket_name: str
    s3_navigator_bucket_name: str
    s3_arcadis_pmd_bucket_name: str
    lambda_memory_mb: int
    lambda_timeout_seconds: int
    api_name: str
    efs_id: str
    efs_access_point_id: str
    neptune_s3_role_name: str
    factors_file_key: str
    base_db_url: str
    loader_url: str
    query_url: str
    arcadis_api_url_root: str
    waze_feed_urls: dict[str, str]
    codecommit_repo_name: str
    custom_domain_name: Optional[str] = None
    certificate_id: Optional[str] = None
    hosted_zone_id: Optional[str] = None
    hosted_zone_name: Optional[str] = None


# Define configurations for each stage
STAGE_CONFIGS = {
    "dev": StageConfig(
        stage_name="dev",
        vpc_id="<dev-vpc-id>",
        sg_id="<dev-security-group-id>",
        efs_sg_id="<dev-efs-security-group-id>",
        neptune_cluster_id="<dev-neptune-cluster-id>",
        neptune_cluster_resource_id="<dev-neptune-resource-id>",
        neptune_endpoint="<dev-neptune-endpoint>",
        s3_upload_bucket_name="<dev-upload-bucket>",
        s3_admin_export_bucket_name="<dev-admin-export-bucket>",
        s3_public_export_bucket_name="<dev-public-export-bucket>",
        s3_navigator_bucket_name="<dev-navigator-bucket>", # legacy bucket
        s3_arcadis_pmd_bucket_name="<dev-arcadis-pmd-bucket>", # legacy bucket
        lambda_memory_mb=256,
        lambda_timeout_seconds=30,
        api_name="<dev-api-name>",
        efs_id="<dev-efs-id>",
        efs_access_point_id="<dev-efs-access-point-id>",
        neptune_s3_role_name="<dev-neptune-s3-role-name>",
        factors_file_key="factors.csv",
        base_db_url="https://<dev-neptune-endpoint>:8182",
        loader_url="https://<dev-neptune-endpoint>:8182/loader",
        query_url="bolt://<dev-neptune-endpoint>:8182",
        arcadis_api_url_root="https://<arcadis-api-id>.execute-api.<region>.amazonaws.com/<base-path>",
        waze_feed_urls={"example-grid": "https://example.invalid/waze/example-grid?format=1"},
        custom_domain_name="<dev-custom-domain>",
        certificate_id="<dev-certificate-id>",
        hosted_zone_id="<dev-hosted-zone-id>",
        hosted_zone_name="<dev-hosted-zone-name>",
        codecommit_repo_name="<dev-codecommit-repository>", # CodeCommit repo
    ),
    "stage": StageConfig(
        stage_name="stage",
        vpc_id="<stage-vpc-id>",
        sg_id="<stage-security-group-id>",
        efs_sg_id="<stage-efs-security-group-id>",
        neptune_cluster_id="<stage-neptune-cluster-id>",
        neptune_cluster_resource_id="<stage-neptune-resource-id>",
        neptune_endpoint="<stage-neptune-endpoint>",
        s3_upload_bucket_name="<stage-upload-bucket>",
        s3_admin_export_bucket_name="<stage-admin-export-bucket>",
        s3_public_export_bucket_name="<stage-public-export-bucket>",
        s3_navigator_bucket_name="<stage-navigator-bucket>", # legacy bucket
        s3_arcadis_pmd_bucket_name="<stage-arcadis-pmd-bucket>", # legacy bucket
        lambda_memory_mb=512,
        lambda_timeout_seconds=60,
        api_name="<stage-api-name>",
        efs_id="<stage-efs-id>",
        efs_access_point_id="<stage-efs-access-point-id>",
        neptune_s3_role_name="<stage-neptune-s3-role-name>",
        factors_file_key="factors.csv",
        base_db_url="https://<stage-neptune-endpoint>:8182",
        loader_url="https://<stage-neptune-endpoint>:8182/loader",
        query_url="bolt://<stage-neptune-endpoint>:8182",
        arcadis_api_url_root="https://<arcadis-api-id>.execute-api.<region>.amazonaws.com/<base-path>",
        waze_feed_urls={"example-grid": "https://example.invalid/waze/example-grid?format=1"},
        custom_domain_name="<stage-custom-domain>",
        certificate_id="<stage-certificate-id>",
        hosted_zone_id="<stage-hosted-zone-id>",
        hosted_zone_name="<stage-hosted-zone-name>",
        codecommit_repo_name="<stage-codecommit-repository>"
    ),
    "prod": StageConfig(
        stage_name="prod",
        vpc_id="<prod-vpc-id>",
        sg_id="<prod-security-group-id>",
        efs_sg_id="<prod-efs-security-group-id>",
        neptune_cluster_id="<prod-neptune-cluster-id>",
        neptune_cluster_resource_id="<prod-neptune-resource-id>",
        neptune_endpoint="<prod-neptune-endpoint>",
        s3_upload_bucket_name="<prod-upload-bucket>",
        s3_admin_export_bucket_name="<prod-admin-export-bucket>",
        s3_public_export_bucket_name="<prod-public-export-bucket>",
        s3_navigator_bucket_name="<prod-navigator-bucket>", # legacy bucket
        s3_arcadis_pmd_bucket_name="<prod-arcadis-pmd-bucket>", # legacy bucket
        lambda_memory_mb=1024,
        lambda_timeout_seconds=120,
        api_name="<prod-api-name>",
        efs_id="<prod-efs-id>",
        efs_access_point_id="<prod-efs-access-point-id>",
        neptune_s3_role_name="<prod-neptune-s3-role-name>",
        factors_file_key="factors.csv",
        base_db_url="https://<prod-neptune-endpoint>:8182",
        loader_url="https://<prod-neptune-endpoint>:8182/loader",
        query_url="bolt://<prod-neptune-endpoint>:8182",
        arcadis_api_url_root="https://<arcadis-api-id>.execute-api.<region>.amazonaws.com/<base-path>",
        waze_feed_urls={"example-grid": "https://example.invalid/waze/example-grid?format=1"},
        custom_domain_name="<prod-custom-domain>",
        certificate_id="<prod-certificate-id>",
        hosted_zone_id="<prod-hosted-zone-id>",
        hosted_zone_name="<prod-hosted-zone-name>",
        codecommit_repo_name="<prod-codecommit-repository>"
    ),
}


def get_config(stage: str) -> StageConfig:
    """Get configuration for a specific stage."""
    if stage not in STAGE_CONFIGS:
        raise ValueError(f"Unknown stage: {stage}. Valid stages: {list(STAGE_CONFIGS.keys())}")
    return STAGE_CONFIGS[stage]
