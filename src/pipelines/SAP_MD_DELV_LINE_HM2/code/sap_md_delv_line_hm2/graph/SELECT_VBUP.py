from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *

def SELECT_VBUP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("VBELN"), 
        col("POSNR"), 
        col("UVFAK"), 
        col("BESTA"), 
        col("UVVLK"), 
        col("LFSTA"), 
        col("LFGSA"), 
        col("WBSTA"), 
        col("UVALL"), 
        col("FKSAA"), 
        col("UVPRS"), 
        col("GBSTA"), 
        col("RFSTA"), 
        col("RFGSA"), 
        col("ABSTA"), 
        col("FSSTA"), 
        col("LSSTA"), 
        col("DELIV_RELTD_BILLG_STA")
    )
