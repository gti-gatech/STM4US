from dataclasses import dataclass


@dataclass
class ApiRoute:
    """Configuration for a single API route."""
    path: str
    method: str
    lambda_id: str
    description: str
    requires_auth: bool = True
    request_parameters: dict | None = None
    request_model: str | None = None


API_ROUTES = [
    # /api/bulk-load
    ApiRoute(
        path="/api/bulk-load",
        method="GET",
        lambda_id="check-bulk-load",
        description="Check bulk load status",
        requires_auth=True,
    ),
    ApiRoute(
        path="/api/bulk-load",
        method="PUT",
        lambda_id="bulk-load",
        description="Initiate bulk load",
        requires_auth=True,
    ),
    
    # /api/bulk-load/validate
    ApiRoute(
        path="/api/bulk-load/validate",
        method="GET",
        lambda_id="validate-sidewalk-data",
        description="Validate sidewalk data",
        requires_auth=True,
        request_parameters={"method.request.querystring.location": True},
    ),
    
    # /api/impedance/base-calculate
    ApiRoute(
        path="/api/impedance/base-calculate",
        method="POST",
        lambda_id="base-impedance-calculation",
        description="Calculate base impedance",
        requires_auth=True,
    ),
    
    # /api/impedance/calculate
    ApiRoute(
        path="/api/impedance/calculate",
        method="POST",
        lambda_id="calculate-impedance",
        description="Calculate impedance with real-time data (async)",
        requires_auth=True,
    ),
    
    # /api/impedance/export
    ApiRoute(
        path="/api/impedance/export",
        method="GET",
        lambda_id="export-impedance",
        description="Export impedance data",
        requires_auth=True,
    ),
    
    # /api/impedance/import
    ApiRoute(
        path="/api/impedance/import",
        method="POST",
        lambda_id="import-impedance",
        description="Import impedance data",
        requires_auth=True,
    ),
    
    # /api/navigator-events
    ApiRoute(
        path="/api/navigator-events",
        method="DELETE",
        lambda_id="import-navigator",
        description="Delete navigator events",
        requires_auth=True,
    ),
    
    # /api/navigator-events/import
    ApiRoute(
        path="/api/navigator-events/import",
        method="POST",
        lambda_id="import-navigator",
        description="Import navigator events",
        requires_auth=True,
        request_model="RequestBodyIdModel",
    ),
    
    # /api/osm
    ApiRoute(
        path="/api/osm",
        method="DELETE",
        lambda_id="bulk-loader-osm",
        description="Delete OSM data",
        requires_auth=True,
    ),
    
    # /api/osm/import
    ApiRoute(
        path="/api/osm/import",
        method="GET",
        lambda_id="bulk-loader-osm",
        description="Get OSM import status",
        requires_auth=True,
    ),
    ApiRoute(
        path="/api/osm/import",
        method="POST",
        lambda_id="bulk-loader-osm",
        description="Process OSM data",
        requires_auth=True,
    ),
    ApiRoute(
        path="/api/osm/import",
        method="PUT",
        lambda_id="bulk-loader-osm",
        description="Ingest OSM data to Neptune",
        requires_auth=True,
    ),

    # /api/pmd/upload
    ApiRoute(
        path="/api/pmd/upload",
        method="POST",
        lambda_id="retrieve-pmd-arcadis",
        description="Retrieve and upload PMD data from Arcadis",
        requires_auth=True,
         request_parameters={"method.request.querystring.date": False},
    ),
    
    # /api/pmd/daily-calculate
    ApiRoute(
        path="/api/pmd/daily-calculate",
        method="POST",
        lambda_id="calculate-performance-metrics",
        description="Calculate daily performance metrics",
        requires_auth=True,
        request_parameters={"method.request.querystring.date": False},
    ),

    # /api/pmd/weekly-aggregate
    ApiRoute(
        path="/api/pmd/weekly-aggregate",
        method="POST",
        lambda_id="aggregate-performance-metrics",
        description="Aggregate daily performance metrics",
        requires_auth=True,
        request_parameters={"method.request.querystring.week": False},
    ),

    # /api/pmd/delete
    ApiRoute(
        path="/api/pmd/delete",
        method="DELETE",
        lambda_id="delete-pmd-arcadis",
        description="Delete PMD data from Arcadis on S3",
        requires_auth=True,
        request_parameters={"method.request.querystring.date": False},
    ),
    
    # /api/vds/import
    ApiRoute(
        path="/api/vds/import",
        method="POST",
        lambda_id="import-vds",
        description="Import VDS data",
        requires_auth=True,
    ),
    
    # /api/waze-alerts
    ApiRoute(
        path="/api/waze-alerts",
        method="DELETE",
        lambda_id="import-waze",
        description="Delete Waze alerts",
        requires_auth=True,
    ),
    
    # /api/waze-alerts/import
    ApiRoute(
        path="/api/waze-alerts/import",
        method="POST",
        lambda_id="import-waze",
        description="Import Waze alerts",
        requires_auth=True,
        request_model="RequestBodyIdModel",
    ),
    
    # NOTE: /{bucket}/{filename} endpoints are direct S3 integrations
    # These should be configured separately in api_gateway.py with S3 integration
    # instead of Lambda integration. They are not included in this list.
]
