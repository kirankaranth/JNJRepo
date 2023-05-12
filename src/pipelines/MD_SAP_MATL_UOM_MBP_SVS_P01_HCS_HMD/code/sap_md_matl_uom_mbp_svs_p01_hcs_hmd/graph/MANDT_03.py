from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_uom_mbp_svs_p01_hcs_hmd.config.ConfigStore import *
from sap_md_matl_uom_mbp_svs_p01_hcs_hmd.udfs.UDFs import *

def MANDT_03(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))) & (col("SPRAS") == lit("E")))
    )
