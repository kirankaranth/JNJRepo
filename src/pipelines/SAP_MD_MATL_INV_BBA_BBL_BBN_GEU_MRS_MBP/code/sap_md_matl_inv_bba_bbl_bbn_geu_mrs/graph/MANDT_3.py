from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.config.ConfigStore import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.udfs.UDFs import *

def MANDT_3(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("_deleted_") == lit("F")) & (col("MANDT") == lit(Config.MANDT))))
