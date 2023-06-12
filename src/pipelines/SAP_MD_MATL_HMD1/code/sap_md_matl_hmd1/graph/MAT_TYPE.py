from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd1.config.ConfigStore import *
from sap_md_matl_hmd1.udfs.UDFs import *

def MAT_TYPE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("cabn_filter") == lit("TYPE_OF_MATERIAL")))
