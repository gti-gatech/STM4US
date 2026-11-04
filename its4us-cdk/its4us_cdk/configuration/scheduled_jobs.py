from dataclasses import dataclass
from aws_cdk import Duration


@dataclass
class ScheduledJob:
    """Configuration for a scheduled Lambda job."""
    lambda_id: str
    name: str
    description: str
    payload: dict | None = None
    fixed_rate: Duration | None = None
    cron_weekday: str | None = None
    cron_hour: str | None = None
    cron_minute: str | None = None


# TODO: Define your scheduled jobs here
# Map schedules to Lambda IDs from lambdas.py
SCHEDULED_JOBS = [

    # waze ingestion scheduled jobs
    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-34.0N84.4W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "34.0N84.4W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
     ),
    
    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.8N84.4W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.8N84.4W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.9N84.4W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.9N84.4W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.8N84.1W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.8N84.1W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-34.0N84.3W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "34.0N84.3W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.9N84.3W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.9N84.3W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.8N84.2W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.8N84.2W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-34.0N84.1W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "34.0N84.1W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.9N84.1W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.9N84.1W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-34.0N84.0W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "34.0N84.0W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-34.0N84.2W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "34.0N84.2W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.9N84.2W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.9N84.2W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.9N84.0W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.9N84.0W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    ScheduledJob(
        lambda_id="import-waze",
        name="import-waze-33.8N84.3W",
        description="Ingest waze data to Neptune every minute",
        payload={"body": {"id": "33.8N84.3W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(1),
    ),

    # navigator ingestion scheduled jobs
    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-34.0N84.4W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "34.0N84.4W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.8N84.4W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.8N84.4W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.9N84.4W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.9N84.4W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.8N84.1W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.8N84.1W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-34.0N84.3W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "34.0N84.3W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.9N84.3W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.9N84.3W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.8N84.2W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.8N84.2W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-34.0N84.1W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "34.0N84.1W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.9N84.1W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.9N84.1W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-34.0N84.0W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "34.0N84.0W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-34.0N84.2W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "34.0N84.2W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.9N84.2W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.9N84.2W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.9N84.0W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.9N84.0W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    ScheduledJob(
        lambda_id="import-navigator",
        name="import-navigator-33.8N84.3W",
        description="Ingest NaviGAtor data to Neptune every five minutes",
        payload={"body": {"id": "33.8N84.3W"}, 
                 "httpMethod": "POST"},
        fixed_rate=Duration.minutes(5),
    ),

    # waze delete scheduled job
    ScheduledJob(
        lambda_id="import-waze",
        name="delete-waze",
        description="Delete waze data from Neptune every minute",
        payload={"httpMethod": "DELETE"},
        fixed_rate=Duration.minutes(1),
    ),

    # navigator delete scheduled job
    ScheduledJob(
        lambda_id="import-navigator",
        name="delete-navigator",
        description="Delete NaviGAtor data from Neptune every five minutes",
        payload={"httpMethod": "DELETE"},
        fixed_rate=Duration.minutes(5),
    ),

    # Arcadis data retrieval scheduled job
    ScheduledJob(
        lambda_id="retrieve-pmd-arcadis",
        name="retrieve-pmd-arcadis-data",
        description="Retrieve Arcadis data for PMD calculation every day at 9:00 UTC",
         payload=None,
        cron_weekday="*", # everyday
        cron_hour="8",    # 8:55 UTC (3:55 AM EST in winter, 4:55 AM EST in summer)
        cron_minute="55",
    ),

    # performance metrics calculation scheduled job
    ScheduledJob(
        lambda_id="calculate-performance-metrics",
        name="calculate-performance-metrics",
        description="Compute performance metrics daily at 9:30 UTC",
        cron_weekday="*", # everyday
        cron_hour="9",   
        cron_minute="30", # 9:30 UTC (4:30 AM EST in winter, 5:30 AM EST in summer)
    ),

    # performance metrics aggregation scheduled job
    ScheduledJob(
        lambda_id="aggregate-performance-metrics",
        name="aggregate-performance-metrics",
        description="Aggregate performance metrics every Monday at 10:00 UTC",
        cron_weekday="MON", # Monday
        cron_hour="10",   # 10:00 UTC (5 AM EST in winter, 6 AM EST in summer)
        cron_minute="0",
    ),

    # Arcadis data deletion scheduled job
    ScheduledJob(
        lambda_id="delete-pmd-arcadis",
        name="delete-pmd-arcadis",
        description="Check and delete Arcadis data daily at 18:00 UTC",
        cron_weekday="*", # everyday
        cron_hour="18",   
        cron_minute="0", # 18:00 UTC (1:00 PM EST in winter, 2:00 PM EST in summer)
    ),

    # NOTE: temporary comment out the schedulers until they are requested
    # VDS ingestion scheduled job
    #ScheduledJob(
    #    lambda_id="import-vds",
    #    name="import-vds",
    #    description="Ingest VDS data to Neptune every five minutes",
    #    fixed_rate=Duration.minutes(5),
    #),

    # impedance calculation scheduled job
    #ScheduledJob(
    #    lambda_id="calculate-impedance-helper",
    #    name="calculate-impedance",
    #    description="Calculate impedance every 15 days",
    #    payload={"body": {"id": "area-1"}},
    #    fixed_rate=Duration.days(15),
    #),
    

]
