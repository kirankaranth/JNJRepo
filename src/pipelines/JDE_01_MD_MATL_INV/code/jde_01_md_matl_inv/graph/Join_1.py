from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.IMITM") == col("in1.LIITM")), "inner")\
        .select(col("in0.IMLITM").alias("MATL_NUM"), col("in1.LIMCU").alias("PLNT_CD"), col("in1.LILOCN").alias("SLOC_CD"), col("in1.LILOTN").alias("BTCH_NUM"), col("in1.LILOTS").alias("BTCH_STS_CD"), col("in1.XFRM_SPCL_STCK_IND").alias("SPCL_STCK_IND"), col("in1.XFRM_PRTY_NUM").alias("PRTY_NUM"), col("in1.XFRM_RSTRCTD_STCK").alias("UNRSTRCTD_STCK"), col("in1.LIQTTR").alias("IN_TRNSFR_STCK "), col("in1.LIQTIN").alias("QLTY_INSP_STCK "), col("in1.XFRM_UNRSTRCTD_STCK").alias("RSTRCTD_STCK"), lit(None).alias("BLCKD_STCK"), lit(None).alias("CRT_DTTM"), lit(None).alias("RTRNS  "), col("in0.IMUOM1").alias("BASE_UOM_CD"), lit(None).alias("STO_IN_TRNST_QTY"), lit(None).alias("PLNT_IN_TRNST_QTY"), lit(None).alias("FISC_YR_OF_CUR_PER"), lit(None).alias("CUR_PER"))
