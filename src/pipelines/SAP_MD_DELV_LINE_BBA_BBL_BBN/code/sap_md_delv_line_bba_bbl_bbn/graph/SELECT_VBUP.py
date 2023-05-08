from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bba_bbl_bbn.config.ConfigStore import *
from sap_md_delv_line_bba_bbl_bbn.udfs.UDFs import *

def SELECT_VBUP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("VBELN"), 
        col("POSNR"), 
        col("UVFAK"), 
        col("BESTA"), 
        col("FKSTA"), 
        col("UVVLK"), 
        col("LFSTA"), 
        col("LFGSA"), 
        col("UVWAK"), 
        col("WBSTA"), 
        col("FKIVP"), 
        col("UVALL"), 
        col("FKSAA"), 
        col("UVPAK"), 
        col("PKSTA"), 
        col("KOQUA"), 
        col("UVPIK"), 
        col("KOSTA"), 
        col("UVPRS"), 
        col("GBSTA"), 
        col("RFSTA"), 
        col("RFGSA"), 
        col("ABSTA"), 
        col("LVSTA"), 
        col("FSSTA"), 
        col("LSSTA")
    )
