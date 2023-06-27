from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_inv_p01_svs.config.ConfigStore import *
from sap_md_matl_inv_p01_svs.udfs.UDFs import *

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
        col("LBUML"), 
        col("_deleted_")
    )
