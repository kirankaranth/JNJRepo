from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.config.ConfigStore import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.udfs.UDFs import *

def RE_NAME(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("AUFNR").alias("AFKO_AUFNR"), 
        col("ABARB").alias("AFKO_ABARB"), 
        col("APLZT").alias("AFKO_APLZT"), 
        col("ARSNR").alias("AFKO_ARSNR"), 
        col("ARSPS").alias("AFKO_ARSPS"), 
        col("AUFLD").alias("AFKO_AUFLD"), 
        col("AUFPL").alias("AFKO_AUFPL"), 
        col("AUFPT").alias("AFKO_AUFPT"), 
        col("BEDID").alias("AFKO_BEDID"), 
        col("CUOBJ").alias("AFKO_CUOBJ"), 
        col("DISPO").alias("AFKO_DISPO"), 
        col("FEVOR").alias("AFKO_FEVOR"), 
        col("FHORI").alias("AFKO_FHORI"), 
        col("FREIZ").alias("AFKO_FREIZ"), 
        col("FTRMI").alias("AFKO_FTRMI"), 
        col("FTRMP").alias("AFKO_FTRMP"), 
        col("FTRMS").alias("AFKO_FTRMS"), 
        col("GAMNG").alias("AFKO_GAMNG"), 
        col("GASMG").alias("AFKO_GASMG"), 
        col("GETRI").alias("AFKO_GETRI"), 
        col("GEUZI").alias("AFKO_GEUZI"), 
        col("GLTPP").alias("AFKO_GLTPP"), 
        col("GLTPS").alias("AFKO_GLTPS"), 
        col("GLTRI").alias("AFKO_GLTRI"), 
        col("GLTRP").alias("AFKO_GLTRP"), 
        col("GLTRS").alias("AFKO_GLTRS"), 
        col("GLUPP").alias("AFKO_GLUPP"), 
        col("GLUPS").alias("AFKO_GLUPS"), 
        col("GLUZP").alias("AFKO_GLUZP"), 
        col("GLUZS").alias("AFKO_GLUZS"), 
        col("GMEIN").alias("AFKO_GMEIN"), 
        col("GSTPP").alias("AFKO_GSTPP"), 
        col("GSTPS").alias("AFKO_GSTPS"), 
        col("GSTRI").alias("AFKO_GSTRI"), 
        col("GSTRP").alias("AFKO_GSTRP"), 
        col("GSTRS").alias("AFKO_GSTRS"), 
        col("GSUPP").alias("AFKO_GSUPP"), 
        col("GSUPS").alias("AFKO_GSUPS"), 
        col("GSUZI").alias("AFKO_GSUZI"), 
        col("GSUZP").alias("AFKO_GSUZP"), 
        col("GSUZS").alias("AFKO_GSUZS"), 
        col("IASMG").alias("AFKO_IASMG"), 
        col("IGMNG").alias("AFKO_IGMNG"), 
        col("KLVARI").alias("AFKO_KLVARI"), 
        col("LKNOT").alias("AFKO_LKNOT"), 
        col("LODIV").alias("AFKO_LODIV"), 
        col("MZAEHL").alias("AFKO_MZAEHL"), 
        col("PAENR").alias("AFKO_PAENR"), 
        col("PDATV").alias("AFKO_PDATV"), 
        col("PLAUF").alias("AFKO_PLAUF"), 
        col("PLGRP").alias("AFKO_PLGRP"), 
        col("PLNAL").alias("AFKO_PLNAL"), 
        col("PLNAW").alias("AFKO_PLNAW"), 
        col("PLNBEZ").alias("AFKO_PLNBEZ"), 
        col("PLNME").alias("AFKO_PLNME"), 
        col("PLNNR").alias("AFKO_PLNNR"), 
        col("PLNTY").alias("AFKO_PLNTY"), 
        col("PLSVB").alias("AFKO_PLSVB"), 
        col("PLSVN").alias("AFKO_PLSVN"), 
        col("PRONR").alias("AFKO_PRONR"), 
        col("PRUEFLOS").alias("AFKO_PRUEFLOS"), 
        col("PVERW").alias("AFKO_PVERW"), 
        col("RKNOT").alias("AFKO_RKNOT"), 
        col("RMZHL").alias("AFKO_RMZHL"), 
        col("RSHID").alias("AFKO_RSHID"), 
        col("RSNID").alias("AFKO_RSNID"), 
        col("RSNUM").alias("AFKO_RSNUM"), 
        col("RUECK").alias("AFKO_RUECK"), 
        col("SAENR").alias("AFKO_SAENR"), 
        col("SBMEH").alias("AFKO_SBMEH"), 
        col("SBMNG").alias("AFKO_SBMNG"), 
        col("SDATV").alias("AFKO_SDATV"), 
        col("SICHZ").alias("AFKO_SICHZ"), 
        col("SLSBS").alias("AFKO_SLSBS"), 
        col("SLSVN").alias("AFKO_SLSVN"), 
        col("STLAL").alias("AFKO_STLAL"), 
        col("STLAN").alias("AFKO_STLAN"), 
        col("STLBEZ").alias("AFKO_STLBEZ"), 
        col("STLNR").alias("AFKO_STLNR"), 
        col("STLST").alias("AFKO_STLST"), 
        col("STLTY").alias("AFKO_STLTY"), 
        col("STUFE").alias("AFKO_STUFE"), 
        col("TERKZ").alias("AFKO_TERKZ"), 
        col("UPTER").alias("AFKO_UPTER"), 
        col("VORGZ").alias("AFKO_VORGZ"), 
        col("VWEGX").alias("AFKO_VWEGX"), 
        col("WEGXX").alias("AFKO_WEGXX"), 
        col("ZAEHL").alias("AFKO_ZAEHL"), 
        col("ZKRIZ").alias("AFKO_ZKRIZ"), 
        col("KLVARP").alias("AFKO_KLVARP")
    )