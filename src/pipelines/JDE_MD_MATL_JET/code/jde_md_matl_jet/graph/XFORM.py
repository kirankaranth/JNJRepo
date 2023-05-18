from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("MATL_NUM", col("IMLITM"))\
        .withColumn("BASE_UOM_CD", trim(col("IMUOM1")))\
        .withColumn("TOT_SHLF_LIF_DAYS_CNT", col("IMSLD").cast(DecimalType(18, 4)))\
        .withColumn("MATL_STS_CD", trim(col("IMSTKT")))\
        .withColumn("MATL_DOC_NUM", trim(col("IMDRAW")))\
        .withColumn("MATL_DOC_VERS_NUM", trim(col("IMRVNO")))\
        .withColumn("MATL_CATLG_NUM", trim(col("IMAITM")))\
        .withColumn("CHG_BY", trim(col("IMUSER")))\
        .withColumn("WT_UNIT", trim(col("IMUWUM")))\
        .withColumn("DOC_TYPE", trim(col("IMLNTY")))\
        .withColumn("SHRT_MATL_NUM", trim(col("IMITM")))\
        .withColumn(
          "LAST_CHG_DT_TIME_DTTM",
          when(
              (length(col("imtday")) == lit(6)), 
              to_timestamp(
                concat(
                  date_add(
                    substring((col("IMUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
                    (substring(col("IMUPMJ"), 4, 3).cast(IntegerType()) - lit(1))
                  ), 
                  lit(""), 
                  col("imtday")
                ), 
                "yyyy-MM-ddHHmmss"
              )
            )\
            .when(
              (length(col("imtday")) == lit(5)), 
              to_timestamp(
                concat(
                  date_add(
                    substring((col("IMUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
                    (substring(col("IMUPMJ"), 4, 3).cast(IntegerType()) - lit(1))
                  ), 
                  lit(""), 
                  concat(lit(0), col("imtday"))
                ), 
                "yyyy-MM-ddHHmmss"
              )
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("IMUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("IMUPMJ"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("SER_LVL", trim(col("IMMPST")))\
        .withColumn("SER_TYPE", trim(col("IMPTSC")))\
        .withColumn("BRAVO_MINOR_CODE_DESC", lookup("BRAVO_D_LU", trim(col("B_M_LU"))).getField("DRDL01"))\
        .withColumn("FRAN_CD_DESC", lookup("FRAN_LU", trim(col("F_C_LU"))).getField("DRDL01"))\
        .withColumn("MATL_GRP_DESC", lookup("MATL_GR_LU", trim(col("M_G_LU"))).getField("DRDL01"))\
        .withColumn("MATL_TYPE_DESC", lookup("MATL_T_LU", trim(col("M_T_D_LU"))).getField("DRDL01"))\
        .withColumn("MATL_TYPE_CD", expr(Config.MATL_TYPE_CD).cast(StringType()))\
        .withColumn("BRND_CD", expr(Config.BRND_CD).cast(StringType()))\
        .withColumn("FRANCHISE_CD", expr(Config.FRANCHISE_CD).cast(StringType()))\
        .withColumn("LCL_PLNG_SUB_FRAN_CD", expr(Config.LCL_PLNG_SUB_FRAN_CD).cast(StringType()))\
        .withColumn("MATL_GRP_CD", expr(Config.MATL_GRP_CD).cast(StringType()))\
        .withColumn("INDSTR_SECTR_CD", expr(Config.INDSTR_SECTR_CD).cast(StringType()))\
        .withColumn("MIN_SHLF_RMN_LIF_DAYS_CNT", expr(Config.MIN_SHLF_RMN_LIF_DAYS_CNT).cast(DecimalType(18, 4)))\
        .withColumn("DSTN_CHN_STS_CD", expr(Config.DSTN_CHN_STS_CD).cast(StringType()))\
        .withColumn("PROD_HIER_CD", expr(Config.PROD_HIER_CD).cast(StringType()))\
        .withColumn("STRG_CONDS_CD", expr(Config.STRG_CONDS_CD).cast(StringType()))\
        .withColumn("BTCH_MNG_IND", expr(Config.BTCH_MNG_IND).cast(StringType()))\
        .withColumn("MATL_SHRT_DESC", expr(Config.MATL_SHRT_DESC).cast(StringType()))\
        .withColumn("SRC_SECTR_CD", expr(Config.SRC_SECTR_CD).cast(StringType()))\
        .withColumn("MATL_PARNT_CD", expr(Config.MATL_PARNT_CD).cast(StringType()))\
        .withColumn("MATL_SUB_TYPE_CD", expr(Config.MATL_SUB_TYPE_CD).cast(StringType()))\
        .withColumn("FIN_HIER_BASE_CD", expr(Config.FIN_HIER_BASE_CD).cast(StringType()))\
        .withColumn("IMPLNT_INSTM_IND", expr(Config.IMPLNT_INSTM_IND).cast(StringType()))\
        .withColumn("KIT_IND", expr(Config.KIT_IND).cast(StringType()))\
        .withColumn("DIR_PART_MRKNG_CD", expr(Config.DIR_PART_MRKNG_CD).cast(StringType()))\
        .withColumn("MATL_CAT_GRP_CD", expr(Config.MATL_CAT_GRP_CD).cast(StringType()))\
        .withColumn("PLNG_HIER3_CD", expr(Config.PLNG_HIER3_CD).cast(StringType()))\
        .withColumn("MATL_SPEC_NUM", expr(Config.MATL_SPEC_NUM).cast(StringType()))\
        .withColumn("MATL_SPEC_VERS_NUM", expr(Config.MATL_SPEC_VERS_NUM).cast(StringType()))\
        .withColumn("VOL_UNIT", expr(Config.VOL_UNIT).cast(StringType()))\
        .withColumn("VOL", expr(Config.VOL).cast(DecimalType(18, 4)))\
        .withColumn("EAN_UPC", expr(Config.EAN_UPC).cast(StringType()))\
        .withColumn("MAIN_STRG_LOC", expr(Config.MAIN_STRG_LOC).cast(StringType()))\
        .withColumn("PROD_LINE", expr(Config.PROD_LINE).cast(StringType()))\
        .withColumn("MAKE_BUY_IN", expr(Config.MAKE_BUY_IN).cast(StringType()))\
        .withColumn("TYPE_OF_MATERIAL", expr(Config.TYPE_OF_MATERIAL).cast(StringType()))\
        .withColumn("STERILE", expr(Config.STERILE).cast(StringType()))\
        .withColumn("BRAVO_MINOR_CODE", expr(Config.BRAVO_MINOR_CODE).cast(StringType()))\
        .withColumn("CMMDTY", expr(Config.CMMDTY).cast(StringType()))\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))\
        .withColumn("MATL_SPEC_NUM", lookup("MAT_SPEC_LU", trim(col("XB_T003"))).getField("DRDL01"))\
        .withColumn("TYPE_OF_MATERIAL", lookup("TYPE_O_MAT_LU", trim(col("XB_T162"))).getField("DRDL01"))\
        .withColumn("MATL_GRP_DESC_2", lookup("MATL_GR_2_LU", trim(col("IMPRP4"))).getField("DRDL01"))
