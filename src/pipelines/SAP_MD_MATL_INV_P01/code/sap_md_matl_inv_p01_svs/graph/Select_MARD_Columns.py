from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_p01_svs.config.ConfigStore import *
from sap_md_matl_inv_p01_svs.udfs.UDFs import *

def Select_MARD_Columns(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MATNR"), 
        col("WERKS"), 
        col("LGORT"), 
        col("LABST"), 
        col("UMLME"), 
        col("INSME"), 
        col("EINME"), 
        col("SPEME"), 
        col("ERSDA"), 
        col("RETME"), 
        col("LFGJA"), 
        col("LFMON"), 
        col("_upt_"), 
        col("_deleted_")
    )
