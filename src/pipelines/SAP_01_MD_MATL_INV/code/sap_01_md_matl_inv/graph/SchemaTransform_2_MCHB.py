from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_inv.config.ConfigStore import *
from sap_01_md_matl_inv.udfs.UDFs import *

def SchemaTransform_2_MCHB(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SRC_TBL_NM", lit(Config.DBTABLE2))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("PLNT_CD", col("WERKS"))\
        .withColumn("SLOC_CD", col("LGORT"))\
        .withColumn("BTCH_NUM", col("CHARG"))\
        .withColumn(
          "BTCH_STS_CD",
          lit(
            "#"
          )
        )\
        .withColumn(
          "SPCL_STCK_IND",
          lit(
            "#"
          )
        )\
        .withColumn(
          "PRTY_NUM",
          lit(
            "#"
          )
        )\
        .withColumn("UNRSTRCTD_STCK", col("CLABS").cast(DecimalType(18, 4)))\
        .withColumn("IN_TRNSFR_STCK", col("CUMLM").cast(DecimalType(18, 4)))\
        .withColumn("QLTY_INSP_STCK", col("CINSM").cast(DecimalType(18, 4)))\
        .withColumn("RSTRCTD_STCK", col("CEINM").cast(DecimalType(18, 4)))\
        .withColumn("BLCKD_STCK", col("CSPEM").cast(DecimalType(18, 4)))\
        .withColumn(
          "CRT_DTTM",
          when((col("ERSDA") == lit("00000000")), lit(None))\
            .when((length(col("ERSDA")) < lit(8)), lit(None))\
            .otherwise(to_timestamp(col("ERSDA"), "yyyMMdd"))
        )\
        .withColumn("RTRNS  ", col("CRETM").cast(DecimalType(18, 4)))\
        .withColumn("BASE_UOM_CD", lookup("LU_MARA_MEINS", col("MATNR")).getField("MEINS"))\
        .withColumn("STO_IN_TRNST_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("PLNT_IN_TRNST_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("FISC_YR_OF_CUR_PER", col("LFGJA").cast(IntegerType()))\
        .withColumn("CUR_PER", col("LFMON").cast(IntegerType()))\
        .withColumn("SHRT_MATL_NUM", lit(None))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SRC_TBL_NM', SRC_TBL_NM, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD, 'SLOC_CD', SLOC_CD, 'BTCH_NUM', BTCH_NUM, 'BTCH_STS_CD', BTCH_STS_CD, 'SPCL_STCK_IND', SPCL_STCK_IND, 'PRTY_NUM', PRTY_NUM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SRC_TBL_NM', SRC_TBL_NM, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD, 'SLOC_CD', SLOC_CD, 'BTCH_NUM', BTCH_NUM, 'BTCH_STS_CD', BTCH_STS_CD, 'SPCL_STCK_IND', SPCL_STCK_IND, 'PRTY_NUM', PRTY_NUM)"
              )
            )
          )
        )\
        .withColumn("_deleted_", lit("F"))
