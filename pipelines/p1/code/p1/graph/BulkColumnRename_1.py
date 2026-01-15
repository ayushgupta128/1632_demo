from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from p1.config.ConfigStore import *
from p1.functions import *

def BulkColumnRename_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, expr
    # Create a mapping of old column names to new column names
    rename_mapping = {}
    # Build expressions in the original column order from in0
    all_expressions = []

    for col_name in in0.columns:
        if col_name in rename_mapping:
            # This column is being renamed
            all_expressions.append(col(col_name).alias(rename_mapping[col_name]))
        else:
            # This column remains unchanged
            all_expressions.append(col(col_name))

    # Select all columns in original order
    return in0.select(*all_expressions)
