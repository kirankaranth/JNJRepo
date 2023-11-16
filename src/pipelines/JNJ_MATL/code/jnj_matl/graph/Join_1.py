from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jnj_matl.config.ConfigStore import *
from jnj_matl.udfs.UDFs import *

def Join_1(spark: SparkSession, mara: DataFrame, makt: DataFrame, ausp: DataFrame) -> DataFrame:
    return mara\
        .alias("mara")\
        .join(makt.alias("makt"), (col("mara.MATNR") == col("makt.MATNR")), "outer")\
        .join(ausp.alias("ausp"), (col("mara.MATNR") == col("ausp.OBJEK")), "outer")\
        .select(col("mara.MATNR").alias("MARA_MATNR"), col("mara.MTART").alias("MTART"), col("mara.SPART").alias("SPART"), col("mara.LABOR").alias("LABOR"), col("mara.EKWSL").alias("EKWSL"), col("mara.LVORM").alias("LVORM"), col("mara.MATKL").alias("MATKL"), col("mara.MBRSH").alias("MBRSH"), col("mara.MEINS").alias("MEINS"), col("mara.MHDHB").alias("MHDHB"), col("mara.MHDRZ").alias("MHDRZ"), col("mara.MSTAE").alias("MSTAE"), col("mara.MSTAV").alias("MSTAV"), col("mara.NTGEW").alias("NTGEW"), col("mara.PRDHA").alias("PRDHA"), col("mara.QMPUR").alias("QMPUR"), col("mara.RAUBE").alias("RAUBE"), col("mara.TEMPB").alias("TEMPB"), col("mara.TRAGR").alias("TRAGR"), col("mara.XCHPF").alias("XCHPF"), col("mara.ZEINR").alias("ZEINR"), col("mara.ZEIVR").alias("ZEIVR"), col("makt.MAKTX").alias("MAKTX"), col("mara.ZZMMSSURGTYPE").alias("ZZMMSSURGTYPE"), col("mara.ZZMMSTYPE").alias("ZZMMSTYPE"), col("mara.ZZWERKS").alias("ZZWERKS"), col("mara.ZZMMSFICLASS").alias("ZZMMSFICLASS"), col("mara.ZZMMSTERILE").alias("ZZMMSTERILE"), col("mara.ZZCATNUMBER").alias("ZZCATNUMBER"), col("mara.ZZSECTOR").alias("ZZSECTOR"), col("mara.ZZP2_BASECODE").alias("ZZP2_BASECODE"), col("mara.ZZMATSUB_TYPE").alias("ZZMATSUB_TYPE"), col("mara.ZZSEC_PRDGRP").alias("ZZSEC_PRDGRP"), col("mara.ZZPROD_CAT1").alias("ZZPROD_CAT1"), col("mara.ZZVARIANT").alias("ZZVARIANT"), col("mara.ZZKIT_IND").alias("ZZKIT_IND"), col("mara.ZZMMSTS").alias("ZZMMSTS"), col("mara.MTPOS_MARA").alias("MTPOS_MARA"), col("mara.ZZP3_LOW_LEVEL").alias("ZZP3_LOW_LEVEL"), col("ausp.ATNAM").alias("ATNAM"), col("ausp.ATWRT").alias("ATWRT"))
