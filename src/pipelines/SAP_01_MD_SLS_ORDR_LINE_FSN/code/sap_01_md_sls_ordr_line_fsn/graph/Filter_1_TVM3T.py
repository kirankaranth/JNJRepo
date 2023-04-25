from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_fsn.config.ConfigStore import *
from sap_01_md_sls_ordr_line_fsn.udfs.UDFs import *

def Filter_1_TVM3T(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
