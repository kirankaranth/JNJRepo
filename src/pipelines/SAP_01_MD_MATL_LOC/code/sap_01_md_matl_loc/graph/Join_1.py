from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.mandt") == col("in1.mandt")), "inner")\
        .select(col("in0.MANDT").alias("MANDT"), col("in0.MATNR").alias("MATNR"), col("in0.WERKS").alias("WERKS"), col("in0.AWSLS").alias("AWSLS"), col("in0.KAUTB").alias("KAUTB"), col("in0.PRCTR").alias("PRCTR"), col("in0.KORDB").alias("KORDB"), col("in0.LADGR").alias("LADGR"), col("in0.SSQSS").alias("SSQSS"), col("in0.MINBE").alias("MINBE"), col("in0.LOSGR").alias("LOSGR"), col("in0.SERNP").alias("SERNP"), col("in0.MMSTA").alias("MMSTA"), col("in0.MMSTD").alias("MMSTD"), col("in0.LVORM").alias("LVORM"), col("in0.DISLS").alias("DISLS"), col("in0.DISPO").alias("DISPO"), col("in0.FEVOR").alias("FEVOR"))
