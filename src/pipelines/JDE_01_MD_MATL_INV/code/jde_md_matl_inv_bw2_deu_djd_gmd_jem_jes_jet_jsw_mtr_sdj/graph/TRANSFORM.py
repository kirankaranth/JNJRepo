from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def TRANSFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SRC_TBL_NM", lit(Config.DBTABLE1))\
        .withColumn(
          "MATL_NUM",
          coalesce(
            col("IMLITM"), 
            lit(
              "#"
            )
          )
        )\
        .withColumn("PLNT_CD", col("LIMCU"))\
        .withColumn("SLOC_CD", col("LILOCN"))\
        .withColumn("BTCH_NUM", col("LILOTN"))\
        .withColumn("BTCH_STS_CD", col("LILOTS"))\
        .withColumn(
          "SPCL_STCK_IND",
          when(
              ((length(trim(col("lipbin"))) == lit(0)) | col("lipbin").isNull()), 
              lit(
                "#"
              )
            )\
            .otherwise(col("lipbin"))
        )\
        .withColumn(
          "PRTY_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "UNRSTRCTD_STCK",
          when((trim(coalesce(col("LILOTS"), lit(""))) == lit("")), (col("LIPQOH") / lit(Config.DIVISOR)))\
            .otherwise(lit(0))\
            .cast(DecimalType(18, 4))
        )\
        .withColumn("IN_TRNSFR_STCK", (col("LIQTTR") / lit(Config.DIVISOR)).cast(DecimalType(18, 4)))\
        .withColumn("QLTY_INSP_STCK", (col("LIQTIN") / lit(Config.DIVISOR)).cast(DecimalType(18, 4)))\
        .withColumn(
          "RSTRCTD_STCK",
          when((trim(coalesce(col("LILOTS"), lit(""))) != lit("")), (col("LIPQOH") / lit(Config.DIVISOR)))\
            .otherwise(lit(0))\
            .cast(DecimalType(18, 4))
        )\
        .withColumn("BASE_UOM_CD", trim(col("IMUOM1")))\
        .withColumn("SHRT_MATL_NUM", trim(col("LIITM")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
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
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
