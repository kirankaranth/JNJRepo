from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_mbp.config.ConfigStore import *
from sap_md_matl_inv_mbp.udfs.UDFs import *

def Select_MCHB(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MATNR"), 
        col("WERKS"), 
        col("LGORT"), 
        col("CHARG"), 
        col("CLABS"), 
        col("CUMLM"), 
        col("CRETM"), 
        col("CINSM"), 
        col("CEINM"), 
        col("CSPEM"), 
        col("ERSDA"), 
        col("LFGJA"), 
        col("LFMON"), 
        col("_upt_")
    )
