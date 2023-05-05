from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd.config.ConfigStore import *
from sap_md_matl_hmd.udfs.UDFs import *

def NDL_SLS_TYPE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("cabn_filter") == lit("NDL_SLS_TYPE")))
