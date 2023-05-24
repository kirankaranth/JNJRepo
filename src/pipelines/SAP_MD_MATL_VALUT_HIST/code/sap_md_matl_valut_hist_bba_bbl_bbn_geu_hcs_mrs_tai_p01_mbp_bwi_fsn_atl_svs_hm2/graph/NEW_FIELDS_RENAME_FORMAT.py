from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.config.ConfigStore import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("VALUT_AREA_CD", col("BWKEY"))\
        .withColumn("VALUT_TYPE_CD", col("BWTAR"))\
        .withColumn("PSTNG_YR", col("LFGJA").cast(IntegerType()))\
        .withColumn("PSTNG_PER", col("LFMON").cast(IntegerType()))\
        .withColumn("PRC_CNTL_IND", trim(col("VPRSV")))\
        .withColumn("TOT_STK_QTY", col("LBKUM").cast(DecimalType(18, 4)))\
        .withColumn("TOT_VAL_AMT", col("SALK3").cast(DecimalType(18, 4)))\
        .withColumn("MVG_AVG_PRC_AMT", col("VERPR").cast(DecimalType(18, 4)))\
        .withColumn("PRC_AMT", col("STPRS").cast(DecimalType(18, 4)))\
        .withColumn("PRC_UNIT_NBR", col("PEINH").cast(DecimalType(18, 4)))\
        .withColumn("VALUT_CLS_CD", trim(col("BKLAS")))\
        .withColumn("BASE_UOM_CD", col("MEINS"))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'VALUT_AREA_CD', VALUT_AREA_CD, 'VALUT_TYPE_CD', VALUT_TYPE_CD, 'PSTNG_YR', PSTNG_YR, 'PSTNG_PER', PSTNG_PER)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'VALUT_AREA_CD', VALUT_AREA_CD, 'VALUT_TYPE_CD', VALUT_TYPE_CD, 'PSTNG_YR', PSTNG_YR, 'PSTNG_PER', PSTNG_PER)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))
