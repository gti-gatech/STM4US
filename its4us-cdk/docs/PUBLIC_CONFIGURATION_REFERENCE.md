# Public Repository Configuration Reference

This document lists the sanitized placeholders in the public CDK source and the external resources a deployer would need to provide.

The repository is currently a sanitized source snapshot. It is **not expected to synthesize or deploy successfully without replacing the placeholders and supplying the external resources described below**.

## 1. Stage configuration placeholders

The three example environments are defined in:

```text
its4us_cdk/configuration/stage_config.py
```

Each stage (`dev`, `stage`, and `prod`) has its own placeholder values.

| Configuration field | Example placeholder | What it represents |
|---|---|---|
| `vpc_id` | `<dev-vpc-id>` | Existing VPC containing the application resources |
| `sg_id` | `<dev-security-group-id>` | Lambda/application security group |
| `efs_sg_id` | `<dev-efs-security-group-id>` | EFS security group |
| `neptune_cluster_id` | `<dev-neptune-cluster-id>` | Existing Neptune cluster identifier |
| `neptune_cluster_resource_id` | `<dev-neptune-resource-id>` | Neptune cluster resource identifier |
| `neptune_endpoint` | `<dev-neptune-endpoint>` | Neptune writer endpoint hostname |
| `s3_upload_bucket_name` | `<dev-upload-bucket>` | Bucket receiving uploaded/input data |
| `s3_admin_export_bucket_name` | `<dev-admin-export-bucket>` | Administrative logs and exports bucket |
| `s3_public_export_bucket_name` | `<dev-public-export-bucket>` | Public export bucket |
| `s3_navigator_bucket_name` | `<dev-navigator-bucket>` | Legacy NaviGAtor data bucket |
| `s3_arcadis_pmd_bucket_name` | `<dev-arcadis-pmd-bucket>` | Legacy performance-metrics bucket |
| `api_name` | `<dev-api-name>` | API Gateway API name |
| `efs_id` | `<dev-efs-id>` | Existing EFS filesystem identifier |
| `efs_access_point_id` | `<dev-efs-access-point-id>` | Existing EFS access point identifier |
| `neptune_s3_role_name` | `<dev-neptune-s3-role-name>` | IAM role Neptune uses to read S3 data |
| `base_db_url` | `https://<dev-neptune-endpoint>:8182` | Neptune HTTPS endpoint |
| `loader_url` | `https://<dev-neptune-endpoint>:8182/loader` | Neptune bulk-loader endpoint |
| `query_url` | `bolt://<dev-neptune-endpoint>:8182` | Neptune Bolt query endpoint |
| `arcadis_api_url_root` | `https://<arcadis-api-id>.execute-api.<region>.amazonaws.com/<base-path>` | Arcadis PMD service root URL |
| `waze_feed_urls` | `https://example.invalid/waze/example-grid?format=1` | Mapping of study-area IDs to authorized Waze feed URLs |
| `custom_domain_name` | `<dev-custom-domain>` | Optional API custom domain |
| `certificate_id` | `<dev-certificate-id>` | ACM certificate identifier for the custom domain |
| `hosted_zone_id` | `<dev-hosted-zone-id>` | Route 53 hosted-zone identifier |
| `hosted_zone_name` | `<dev-hosted-zone-name>` | Route 53 hosted-zone name |
| `codecommit_repo_name` | `<dev-codecommit-repository>` | CodeCommit source repository used by pipelines |

The `stage` and `prod` entries use the same fields with the corresponding `<stage-...>` and `<prod-...>` prefixes.

Values that are not stage-specific and can usually remain unchanged include:

- `stage_name`: `dev`, `stage`, or `prod`
- `lambda_memory_mb`
- `lambda_timeout_seconds`
- `factors_file_key`: `factors.csv`
- Resource naming and Lambda behavior flags in `configuration/lambdas.py`

## 2. Arcadis configuration

The PMD retrieval Lambda requires:

```text
ARCADIS_API_URL_ROOT
ARCADIS_API_KEY_SECRET_ID
```

CDK supplies both values to `retrieve-pmd-arcadis`:

- `ARCADIS_API_URL_ROOT` comes from `StageConfig.arcadis_api_url_root`.
- `ARCADIS_API_KEY_SECRET_ID` points to the configured Secrets Manager secret.

The API key itself must not be placed in source code, CDK context, Lambda environment variables, or pipeline parameters as a plaintext value.

The Arcadis endpoint and credentials require an authorized provider relationship and access to the applicable PMD API.

## 3. Waze configuration

The Waze Lambda requires `WAZE_URLS`, a JSON object mapping dataset IDs to feed URLs. The public example is:

```json
{
  "example-grid": "https://example.invalid/waze/example-grid?format=1"
}
```

Replace it with mappings for the study areas that the deployer is authorized to access. The feed URL mapping may contain provider-specific identifiers and should be kept in deployment configuration when those identifiers are not intended for publication.

The active Lambda receives the mapping through its environment. The legacy bulk-load implementation also expects the same `WAZE_URLS` environment variable.

## 4. Secrets Manager resources

The CDK imports, rather than creates, stage-scoped secrets using the current naming convention:

```text
its4us/<stage>/admin-token
its4us/<stage>/client-token
its4us/<stage>/arcadis-api-key
```

A deployer must either create equivalent secrets or change `its4us_cdk/constructs/secrets.py` to use another naming convention.

Required secret contents:

- `admin-token`: token accepted by the API authorizer for administrative operations.
- `client-token`: token accepted by the API authorizer for client operations.
- `arcadis-api-key`: API key used for Arcadis PMD retrieval.

Secret values should be entered through an approved secret-management process. See `docs/SECRETS_MANAGER.md` for the current operational pattern, but do not copy organization-specific values into a public deployment.

## 5. Lambda layers

Lambda definitions in `configuration/lambdas.py` request these layer names:

- `pandas`
- `neo4j`
- `requests`
- `pytz`

Layer ARNs are defined in:

```text
its4us_cdk/configuration/layers.py
```

The public source retains the existing layer configuration. A deployer must ensure that:

- The AWS-managed pandas layer exists for the selected region and Python runtime.
- The custom `neo4j`, `requests`, and `pytz` layers exist in the deployment account and region.
- Layer versions are compatible with Python 3.14 and the Lambda code.
- Any omitted or unavailable dependency is packaged with the Lambda instead.

The layer ARNs are deployment dependencies, not credentials, but they are not guaranteed to exist in another AWS account.

## 6. AWS infrastructure prerequisites

The current CDK code imports existing infrastructure. A deployer needs, at minimum:

### Networking

- A VPC.
- Lambda/application security group.
- EFS security group.
- Private subnets with appropriate egress and connectivity.
- Network access from Lambda subnets to Neptune, S3, Secrets Manager, and external provider APIs as required.

### Neptune

- A Neptune cluster compatible with the application queries.
- Writer endpoint and cluster/resource identifiers.
- Neptune connectivity on port `8182`.
- An IAM role allowing Neptune bulk loads to read the relevant S3 bucket.
- Appropriate Neptune database permissions for the Lambda execution roles.

### S3

The application expects buckets for:

- Upload/input data.
- Administrative logs and exports.
- Public exports.
- Legacy NaviGAtor data.
- Legacy Arcadis PMD data.

Bucket policies and Lambda IAM permissions must match the functions enabled in `configuration/lambdas.py`.

### EFS

- An EFS filesystem.
- An EFS access point.
- Mount targets in the Lambda VPC subnets.
- Security-group rules allowing Lambda-to-EFS traffic.

### Secrets Manager

- The three stage-scoped secrets listed above.
- Lambda execution-role permissions limited to the secrets each function needs.

### API Gateway and DNS

The application creates API Gateway resources and can configure a custom domain. For custom-domain deployment, provide:

- An ACM certificate in the correct region.
- A Route 53 hosted zone.
- Permission to create or update the alias record.

Without those resources, omit the custom-domain configuration or use an API Gateway-managed endpoint in a separately adapted deployment.

## 7. CodePipeline and CodeCommit

The current application behavior is:

- `dev` and `stage`: create PipelineStack resources.
- `prod`: do not create a PipelineStack; perform the existing production pipeline guard.

To use the existing pipeline implementation, provide:

- A CodeCommit repository matching `codecommit_repo_name`.
- Branches named for the configured stages.
- Permissions for CodePipeline, CodeBuild, Lambda, S3, and CloudFormation as used by the pipeline constructs.
- The pipeline test and health-check Lambda functions.
- A deployment role and artifact buckets appropriate for the account.

The pipeline configuration is not needed if the code is being used only as a source reference.

## 8. AWS account and region

The CDK environment is taken from:

```text
CDK_DEFAULT_ACCOUNT
CDK_DEFAULT_REGION
```

The deployment identity must have permission to import and use the configured VPC, Neptune, S3, EFS, Secrets Manager, API Gateway, IAM, Lambda, EventBridge, CloudWatch, and—when enabled—CodeCommit, CodePipeline, and CodeBuild resources.

The account and region must be consistent with the resource identifiers and layer ARNs supplied by the deployer.

## 9. Runtime and build requirements

- Python 3.14.
- AWS CDK v2 and the dependencies in `requirements.txt`.
- CDK CLI configured for the target account and region.
- Lambda dependencies or compatible layers for each function.
- Network access to provider APIs for Waze and Arcadis integrations.
- Appropriate source data in S3, including `factors.csv` and any required Navigator, VDS, OSM, Waze, or PMD inputs.

## 10. Public-repository limitations

The public source intentionally does not provide:

- Existing AWS resource identifiers.
- Existing AWS account information.
- Current organization domains.
- Authorized Waze feed URLs.
- The Arcadis API key or private provider configuration.
- Existing secret values.
- A guarantee that the placeholder CDK configuration will synthesize or deploy.

Anyone implementing the public source must supply these values through their own AWS account, provider agreements, deployment configuration, and secret-management process.
