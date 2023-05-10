from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_bwi.config.ConfigStore import *
from sap_md_matl_inv_bwi.udfs.UDFs import *

def SELECT_MSSL(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MATNR"), 
        col("WERKS"), 
        col("SOBKZ"), 
        col("SLLAB"), 
        col("SLINS"), 
        col("ERSDA"), 
        col("_upt_"), 
        col("LIFNR"), 
        col("SLEIN")
    )
