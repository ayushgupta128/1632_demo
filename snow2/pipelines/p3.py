from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p3", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p3__reformat_1 = Process(name = "p3__Reformat_1", properties = ModelTransform(modelName = "p3__Reformat_1"))

