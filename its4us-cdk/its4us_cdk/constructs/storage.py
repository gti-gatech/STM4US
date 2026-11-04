from constructs import Construct
from aws_cdk import aws_s3 as s3


class StorageConstruct(Construct):
    """
    Imports existing S3 buckets for the application.
    """
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config,
    ):
        super().__init__(scope, construct_id)

        self.upload_bucket = s3.Bucket.from_bucket_name(
            self,
            "UploadBucket",
            config.s3_upload_bucket_name,
        )

        self.public_export_bucket = s3.Bucket.from_bucket_name(
            self,
            "ExportBucket",
            config.s3_public_export_bucket_name,
        )

        self.admin_export_bucket = s3.Bucket.from_bucket_name(
            self,
            "AdminExportBucket",
            config.s3_admin_export_bucket_name,
        )

        # legacy bucket for backward compatibility during migration
        self.navigator_bucket = s3.Bucket.from_bucket_name(
            self,
            "NavigatorBucket",
            config.s3_navigator_bucket_name,
        )

        # legacy bucket for backward compatibility during migration
        self.arcadis_pmd_bucket = s3.Bucket.from_bucket_name(
            self,
            "ArcadisPMDBucket",
            config.s3_arcadis_pmd_bucket_name,
        )

