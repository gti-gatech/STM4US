# ITS4US

ITS4US is an open-source mobility platform intended to help communities improve access to transportation for all travelers, including people with disabilities, older adults, low-income individuals, and people with limited English proficiency.

The repository contains AWS CDK infrastructure and Lambda application code for transportation-network data processing, accessibility analysis, and related APIs.

## Architecture

The CDK application can provision the infrastructure required by the platform, including:

- VPC networking and security groups
- A graph database
- AWS Lambda functions
- Amazon S3 storage
- API Gateway
- Elastic File System storage where required by Lambda functions
- EventBridge schedules
- Optional CI/CD resources

The application processes data from sources such as OpenStreetMap and other transportation data providers. Provider credentials and deployment-specific values must be supplied outside the public source tree.

## Prerequisites

- An AWS account with permissions appropriate for the resources being deployed
- AWS CLI configured for the target account
- Python 3.14 or a compatible supported version
- Node.js and npm
- AWS CDK CLI
- Git

Install the CDK CLI if needed:

```bash
npm install -g aws-cdk
```

## AWS CDK Setup

The CDK application is in `its4us-cdk/`.

```bash
cd its4us-cdk
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows, create and activate the virtual environment with the equivalent commands:

```bat
python -m venv .venv
.venv\Scripts\activate.bat
python -m pip install -r requirements.txt
```

Before the first deployment, bootstrap CDK in the target account and Region:

```bash
cdk bootstrap aws://<account-id>/<region>
```

Review the synthesized template before deployment:

```bash
cdk synth --context stage=<stage>
cdk diff --context stage=<stage>
```

Deploy the stacks for a selected environment:

```bash
cdk deploy --all --context stage=<stage>
```

The valid environment names and their configuration are defined in `its4us-cdk/its4us_cdk/configuration/stage_config.py`. Replace placeholder values there through an approved private configuration process; do not commit account IDs, endpoints, resource IDs, domain names, or credentials to this repository.

Useful commands:

```bash
cdk ls
cdk synth
cdk diff
cdk deploy --all --context stage=<stage>
```

## Manual AWS Console Setup

There are some resources the CDK does not manage, and must be setup manually:

1. Select the target AWS account and Region.
2. Create or identify a VPC with the required subnets, route tables, NAT or internet connectivity, and security groups.
3. Create the graph database inside the VPC and restrict access to approved application security groups.
4. Create the required S3 buckets with Block Public Access enabled unless a documented public export is explicitly required. Enable encryption and lifecycle policies appropriate to the data.

### Secrets

Secrets must be stored in AWS Secrets Manager or another approved secret store. Do not place secret values in source files, CDK context, CloudFormation parameters, Lambda environment variables, API examples, shell history, or CI/CD parameters. See [`its4us-cdk/docs/SECRETS_MANAGER.md`](its4us-cdk/docs/SECRETS_MANAGER.md) for the repository's secret-reference pattern.

### Environment Configuration

Deployment-specific values such as VPC IDs, security-group IDs, database endpoints, bucket names, certificate IDs, hosted-zone information, and repository names are intentionally represented by placeholders in the public source. Keep real values in a private configuration or deployment system.

## Project Layout

```text
its4us-cdk/
├── app.py                         # CDK application entry point
├── its4us_cdk/                    # Stacks, constructs, and configuration
├── aws_lambda/                    # Lambda application code
├── docs/                          # Public CDK and secret-management guidance
└── tests/                         # CDK and Lambda tests
```

## Development

Run the available tests from the CDK directory after installing development dependencies:

```bash
cd its4us-cdk
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
pytest
```

Do not add production exports, raw provider feeds, user data, operational snapshots, credentials, private keys, or deployment-generated files to the repository. Use synthetic fixtures for tests whenever possible.

## References

- [AWS CDK Documentation](https://docs.aws.amazon.com/cdk/)
- [AWS CloudFormation Documentation](https://docs.aws.amazon.com/cloudformation/)
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [Amazon Neptune Documentation](https://docs.aws.amazon.com/neptune/)
- [Amazon S3 Documentation](https://docs.aws.amazon.com/s3/)
