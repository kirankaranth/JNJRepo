from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", col("SRC_SYS_CD"))\
        .withColumn("SRC_TBL_NM", lit(Config.DBTABLE1))\
        .withColumn("MATL_NUM", col("IMLITM"))\
        .withColumn("PLNT_CD", col("LIMCU"))\
        .withColumn("SLOC_CD", col("LILOCN"))\
        .withColumn("BTCH_NUM", col("LILOTN"))\
        .withColumn("BTCH_STS_CD", col("LILOTS"))\
        .withColumn("SPCL_STCK_IND", col("LIPBIN"))\
        .withColumn("PRTY_NUM", col("XFRM_PRTY_NUM"))\
        .withColumn("UNRSTRCTD_STCK", col("XFRM_UNRSTRCTD_STCK"))\
        .withColumn("IN_TRNSFR_STCK", col("XFRM_IN_TRNSFR_STCK"))\
        .withColumn("QLTY_INSP_STCK", col("XFRM_RSTRCTD_STCK"))\
        .withColumn("BLCKD_STCK", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("CRT_DTTM", to_timestamp(lit(None)))\
        .withColumn("STO_IN_TRNST_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("PLNT_IN_TRNST_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("FISC_YR_OF_CUR_PER", lit(None).cast(IntegerType()))\
        .withColumn("CUR_PER", lit(None).cast(IntegerType()))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", to_timestamp(current_timestamp(), "yyyy-MM-dd"))\
        .withColumn("DAI_UPDT_DTTM", to_timestamp(current_timestamp(), "yyyy-MM-dd"))
