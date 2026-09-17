from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
Schedules = [Schedule(
               Name = "sf1",
               emails = ["g.ayush@prophecy.io"],
               emailOnStart = True,
               emailOnFailure = True,
               emailOnSuccess = True,
               versionMode = "latest",
               cron = "0 0/1 * * * ? *",
               timezone = "Asia/Kolkata"
             )]
args = PipelineArgs(label = "p2", version = 1, auto_layout = False, schedules = Schedules)

with Pipeline(args) as pipeline:
    p2__reformat_1 = Process(name = "p2__Reformat_1", properties = ModelTransform(modelName = "p2__Reformat_1"))

