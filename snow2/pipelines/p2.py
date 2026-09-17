from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p2", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    aircraft_fleet_snowflake = Process(
        name = "aircraft_fleet_snowflake",
        properties = Dataset(
          table = Dataset.DBTSource(name = "a1h", sourceType = "Table", sourceName = "AIRLINES_AIRLINES"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        comment = "Loads airline data from the AIRLINES_AIRLINES table for further processing."
    )

