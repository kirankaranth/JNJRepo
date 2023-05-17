from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *

def SELECT_LIPS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("VBELN"), 
        col("POSNR"), 
        col("ARKTX"), 
        col("BWART"), 
        col("VGBEL"), 
        col("KOMKZ"), 
        col("PSTYV"), 
        col("SOBKZ"), 
        col("UECHA"), 
        col("UEPOS"), 
        col("UMVKZ"), 
        col("UMVKN"), 
        col("LFIMG"), 
        col("LGMNG"), 
        col("MEINS"), 
        col("CHARG"), 
        col("HSDAT"), 
        col("SPE_GEN_ELIKZ"), 
        col("VFDAT"), 
        col("MATNR"), 
        col("UMWRK"), 
        col("VGPOS"), 
        col("VRKME"), 
        col("WERKS"), 
        col("LGORT"), 
        col("LICHN"), 
        col("ERDAT"), 
        col("ERZET"), 
        col("POSAR"), 
        col("LFPOS"), 
        col("BRGEW"), 
        col("NTGEW"), 
        col("GEWEI"), 
        col("VOLUM"), 
        col("VOLEH"), 
        col("NETWR"), 
        col("PRODH"), 
        col("MVGR4"), 
        col("VTWEG"), 
        col("MATWA"), 
        col("SPART"), 
        col("VGTYP"), 
        col("MATKL"), 
        col("MTART"), 
        col("KCMENG"), 
        col("ORMNG"), 
        col("_upt_")
    )
