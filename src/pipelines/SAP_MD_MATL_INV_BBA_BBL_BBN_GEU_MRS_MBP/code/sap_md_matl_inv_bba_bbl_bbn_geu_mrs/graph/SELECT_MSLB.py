from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.config.ConfigStore import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.udfs.UDFs import *

def SELECT_MSLB(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MATNR"), 
        col("WERKS"), 
        col("CHARG"), 
        col("SOBKZ"), 
        col("LIFNR"), 
        col("LBLAB"), 
        col("LBINS"), 
        col("LBEIN"), 
        col("ERSDA"), 
        col("_upt_"), 
        col("LBUML")
    )
