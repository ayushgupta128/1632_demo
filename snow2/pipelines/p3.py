from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
Schedules = [Schedule(
               Name = "schedule_fis",
               emails = ["g.ayush@prophecy.io"],
               emailOnStart = True,
               emailOnFailure = True,
               emailOnSuccess = True,
               versionMode = "latest",
               cron = "0 0/1 * * * ? *",
               timezone = "Asia/Kolkata"
             )]
args = PipelineArgs(label = "p3", version = 1, auto_layout = False, schedules = Schedules)

with Pipeline(args) as pipeline:
    p3__reformat_1 = Process(name = "p3__Reformat_1", properties = ModelTransform(modelName = "p3__Reformat_1"))

