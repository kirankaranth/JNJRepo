from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bwi_tai.config.ConfigStore import *
from sap_md_delv_line_bwi_tai.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame, in3: DataFrame) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.VBELN") == col("in1.VBELN")), "left_outer")\
        .join(in2.alias("in2"), (col("in0.VGBEL") == col("in2.VBELN")), "left_outer")\
        .join(
          in3.alias("in3"),
          ((col("in0.VGBEL") == col("in3.VBELN")) & (col("in0.POSNR") == col("in3.POSNR"))),
          "left_outer"
        )\
        .select(col("in0.VBELN").alias("VBELN"), col("in0.POSNR").alias("POSNR"), col("in1.LFART").alias("LFART"), col("in2.BUKRS_VF").alias("BUKRS_VF"), col("in0.ARKTX").alias("ARKTX"), col("in0.BWART").alias("BWART"), col("in0.VGBEL").alias("VGBEL"), col("in0.KOMKZ").alias("KOMKZ"), col("in0.PSTYV").alias("PSTYV"), col("in0.SOBKZ").alias("SOBKZ"), col("in0.UECHA").alias("UECHA"), col("in0.UEPOS").alias("UEPOS"), col("in0.UMVKZ").alias("UMVKZ"), col("in0.UMVKN").alias("UMVKN"), col("in1.WADAT_IST").alias("WADAT_IST"), col("in0.LFIMG").alias("LFIMG"), col("in0.LGMNG").alias("LGMNG"), col("in0.MEINS").alias("MEINS"), col("in0.CHARG").alias("CHARG"), col("in3.UVFAK").alias("UVFAK"), col("in3.BESTA").alias("BESTA"), col("in0.HSDAT").alias("HSDAT"), col("in3.FKSTA").alias("FKSTA"), col("in0.SPE_GEN_ELIKZ").alias("SPE_GEN_ELIKZ"), col("in3.UVVLK").alias("UVVLK"), col("in3.LFSTA").alias("LFSTA"), col("in3.LFGSA").alias("LFGSA"), col("in0.VFDAT").alias("VFDAT"), col("in3.UVWAK").alias("UVWAK"), col("in3.WBSTA").alias("WBSTA"), col("in3.FKIVP").alias("FKIVP"), col("in0.MATNR").alias("MATNR"), col("in3.UVALL").alias("UVALL"), col("in0.ZZCONTAINER").alias("ZZCONTAINER"), col("in3.FKSAA").alias("FKSAA"), col("in3.UVPAK").alias("UVPAK"), col("in3.PKSTA").alias("PKSTA"), col("in3.KOQUA").alias("KOQUA"), col("in3.UVPIK").alias("UVPIK"), col("in3.KOSTA").alias("KOSTA"), col("in3.UVPRS").alias("UVPRS"), col("in3.GBSTA").alias("GBSTA"), col("in0.UMWRK").alias("UMWRK"), col("in3.RFSTA").alias("RFSTA"), col("in3.RFGSA").alias("RFGSA"), col("in3.ABSTA").alias("ABSTA"), col("in0.VGPOS").alias("VGPOS"), col("in0.VRKME").alias("VRKME"), col("in0.WERKS").alias("WERKS"), col("in0.LGORT").alias("LGORT"), col("in0.LICHN").alias("LICHN"), col("in3.LVSTA").alias("LVSTA"), col("in0.ERDAT").alias("ERDAT"), col("in0.ERZET").alias("ERZET"), col("in0.POSAR").alias("POSAR"), col("in0.LFPOS").alias("LFPOS"), col("in0.BRGEW").alias("BRGEW"), col("in0.NTGEW").alias("NTGEW"), col("in0.GEWEI").alias("GEWEI"), col("in0.VOLUM").alias("VOLUM"), col("in0.VOLEH").alias("VOLEH"), col("in0.NETWR").alias("NETWR"), col("in0.PRODH").alias("PRODH"), col("in0.MVGR4").alias("MVGR4"), col("in0.VTWEG").alias("VTWEG"), col("in3.FSSTA").alias("FSSTA"), col("in3.LSSTA").alias("LSSTA"), col("in0.MATWA").alias("MATWA"), col("in0.SPART").alias("SPART"), col("in0.VGTYP").alias("VGTYP"), col("in0.MATKL").alias("MATKL"), col("in0.MTART").alias("MTART"), col("in0.KCMENG").alias("KCMENG"), col("in0.ORMNG").alias("ORMNG"), col("in0._upt_").alias("_upt_"))
