from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line.config.ConfigStore import *
from sap_01_md_sls_ordr_line.udfs.UDFs import *

def Join_1(
        spark: SparkSession,
        VBAP: DataFrame,
        VBAK: DataFrame,
        VBKD: DataFrame, 
        TVAGT: DataFrame, 
        TVM1T: DataFrame, 
        TVM2T: DataFrame, 
        TVM3T: DataFrame, 
        TVM5T: DataFrame, 
        TVSTT: DataFrame, 
        TVST: DataFrame, 
        TVAPT: DataFrame, 
        TVRO: DataFrame, 
        TVROT: DataFrame
) -> DataFrame:
    return VBAP\
        .alias("VBAP")\
        .join(VBAK.alias("VBAK"), (col("VBAP.VBELN") == col("VBAK.VBELN")), "left_outer")\
        .join(
          VBKD.alias("VBKD"),
          ((col("VBAP.VBELN") == col("VBKD.VBELN")) & (col("VBAP.POSNR") == col("VBKD.POSNR"))),
          "left_outer"
        )\
        .join(TVAGT.alias("TVAGT"), (col("VBAP.ABGRU") == col("TVAGT.ABGRU")), "left_outer")\
        .join(TVM1T.alias("TVM1T"), (col("VBAP.MVGR1") == col("TVM1T.MVGR1")), "left_outer")\
        .join(TVM2T.alias("TVM2T"), (col("VBAP.MVGR2") == col("TVM2T.MVGR2")), "inner")\
        .join(TVM3T.alias("TVM3T"), (col("VBAP.MVGR3") == col("TVM3T.MVGR3")), "inner")\
        .join(TVM5T.alias("TVM5T"), (col("VBAP.MVGR5") == col("TVM5T.MVGR5")), "inner")\
        .join(TVSTT.alias("TVSTT"), (col("VBAP.VSTEL") == col("TVSTT.VSTEL")), "inner")\
        .join(TVST.alias("TVST"), (col("VBAP.VSTEL") == col("TVST.VSTEL")), "inner")\
        .join(TVAPT.alias("TVAPT"), (col("VBAP.PSTYV") == col("TVAPT.PSTYV")), "inner")\
        .join(TVRO.alias("TVRO"), (col("VBAP.ROUTE") == col("TVRO.ROUTE")), "inner")\
        .join(TVROT.alias("TVROT"), (col("VBAP.ROUTE") == col("TVROT.ROUTE")), "inner")
