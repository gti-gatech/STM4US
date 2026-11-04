from aws_cdk import aws_lambda as _lambda

# AWS Managed Layers
PANDAS_LAYER_ARN = "arn:aws:lambda:{region}:336392948345:layer:AWSSDKPandas-Python314:7"

# Custom Layer ARNs - UPDATE THESE with your actual layer ARNs
NEO4J_LAYER_ARN = "arn:aws:lambda:{region}:{account}:layer:neo4j-python314:1"
REQUESTS_LAYER_ARN = "arn:aws:lambda:{region}:{account}:layer:requests-python314:1"
PYTZ_LAYER_ARN = "arn:aws:lambda:{region}:{account}:layer:pytz-python314:1"
CHARSET_NORMALIZER_LAYER_ARN = "arn:aws:lambda:{region}:{account}:layer:charset-normalizer-python314:1"
GEOPY_LAYER_ARN = "arn:aws:lambda:{region}:{account}:layer:geopy-python314:1"


def get_pandas_layer(scope, region: str) -> _lambda.ILayerVersion:
    """AWS SDK for pandas (includes pandas, numpy, boto3, etc.)"""
    return _lambda.LayerVersion.from_layer_version_arn(
        scope,
        "PandasLayer",
        PANDAS_LAYER_ARN.format(region=region)
    )

def get_neo4j_layer(scope, region: str, account: str) -> _lambda.ILayerVersion:
    """Neo4j Python driver layer"""
    return _lambda.LayerVersion.from_layer_version_arn(
        scope,
        "Neo4jLayer",
        NEO4J_LAYER_ARN.format(region=region, account=account)
    )

def get_requests_layer(scope, region: str, account: str) -> _lambda.ILayerVersion:
    """Requests HTTP library layer"""
    return _lambda.LayerVersion.from_layer_version_arn(
        scope,
        "RequestsLayer",
        REQUESTS_LAYER_ARN.format(region=region, account=account)
    )

def get_pytz_layer(scope, region: str, account: str) -> _lambda.ILayerVersion:
    """Pytz timezone library layer"""
    return _lambda.LayerVersion.from_layer_version_arn(
        scope,
        "PytzLayer",
        PYTZ_LAYER_ARN.format(region=region, account=account)
    )

def get_charset_normalizer_layer(scope, region: str, account: str) -> _lambda.ILayerVersion:
    """Charset-normalizer library layer"""
    return _lambda.LayerVersion.from_layer_version_arn(
        scope,
        "CharsetNormalizerLayer",
        CHARSET_NORMALIZER_LAYER_ARN.format(region=region, account=account)
    )

def get_geopy_layer(scope, region: str, account: str) -> _lambda.ILayerVersion:
    """Geopy library layer"""
    return _lambda.LayerVersion.from_layer_version_arn(
        scope,
        "GeopyLayer",
        GEOPY_LAYER_ARN.format(region=region, account=account)
    )