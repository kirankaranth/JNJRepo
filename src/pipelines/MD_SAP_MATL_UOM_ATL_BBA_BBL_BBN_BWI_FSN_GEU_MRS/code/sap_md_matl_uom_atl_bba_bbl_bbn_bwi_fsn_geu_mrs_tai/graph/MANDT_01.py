from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.config.ConfigStore import *
from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.udfs.UDFs import *

def MANDT_01(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))))
