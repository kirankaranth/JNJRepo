from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_mfg_order_itm_mbp_bwi_bbn_bbl_bba_mrs_p01_tai.config.ConfigStore import *
from sap_md_mfg_order_itm_mbp_bwi_bbn_bbl_bba_mrs_p01_tai.udfs.UDFs import *

def MANDT_FILTER_AFPO(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("_deleted_") == lit("F")) & (col("MANDT") == lit(Config.MANDT))))
