from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.udfs.UDFs import *

def D_M_OBTAB(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("_deleted_") == lit("F")) & (col("MANDT") == lit(Config.MANDT))) & (col("OBTAB") == lit("MARA")))
    )
