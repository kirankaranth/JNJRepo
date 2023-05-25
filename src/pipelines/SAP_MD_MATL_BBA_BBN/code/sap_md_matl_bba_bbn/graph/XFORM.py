from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bba_bbn.config.ConfigStore import *
from sap_md_matl_bba_bbn.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("MATL_TYPE_CD", trim(col("MTART")))\
        .withColumn("BRND_CD", trim(col("BRAND_ID")))\
        .withColumn("FRANCHISE_CD", trim(col("SPART")))\
        .withColumn("LCL_PLNG_SUB_FRAN_CD", trim(col("LABOR")))\
        .withColumn("PRCHSNG_VAL_KEY_CD", trim(col("EKWSL")))\
        .withColumn("DEL_IND", trim(col("LVORM")))\
        .withColumn("MATL_GRP_CD", trim(col("MATKL")))\
        .withColumn("INDSTR_SECTR_CD", trim(col("MBRSH")))\
        .withColumn("BASE_UOM_CD", trim(col("MEINS")))\
        .withColumn("TOT_SHLF_LIF_DAYS_CNT", col("MHDHB").cast(DecimalType(18, 4)))\
        .withColumn("MIN_SHLF_RMN_LIF_DAYS_CNT", col("MHDRZ").cast(DecimalType(18, 4)))\
        .withColumn("MATL_STS_CD", trim(col("MSTAE")))\
        .withColumn("DSTN_CHN_STS_CD", trim(col("MSTAV")))\
        .withColumn("NET_WT_MEAS", col("NTGEW").cast(DecimalType(18, 4)))\
        .withColumn("PROD_HIER_CD", trim(col("PRDHA")))\
        .withColumn("PRCMT_QUAL_MGMT_IND", trim(col("QMPUR")))\
        .withColumn("STRG_CONDS_CD", trim(col("RAUBE")))\
        .withColumn("LBL_TEMP_RNG", trim(col("TEMPB")))\
        .withColumn("TRSPN_GRP_CD", trim(col("TRAGR")))\
        .withColumn("BTCH_MNG_IND", trim(col("XCHPF")))\
        .withColumn("MATL_DOC_NUM", trim(col("ZEINR")))\
        .withColumn("MATL_DOC_VERS_NUM", trim(col("ZEIVR")))\
        .withColumn("MATL_SHRT_DESC", trim(lookup("LU_MAKT_MAKTX", col("MATNR")).getField("MAKTX")))\
        .withColumn("MATL_CAT_GRP_CD", trim(col("MTPOS_MARA")))\
        .withColumn("MATL_SPEC_NUM", trim(lookup("MAT_SPEC_LU", col("MATNR")).getField("ATWRT")))\
        .withColumn("MATL_SPEC_VERS_NUM", trim(lookup("SPEC_VER_LU", col("MATNR")).getField("ATWRT")))\
        .withColumn("CHG_BY", trim(col("AENAM")))\
        .withColumn("DOC_CHG_NUM", trim(col("AESZN")))\
        .withColumn("CNTNR_REQ", trim(col("BEHVO")))\
        .withColumn("OLD_MATL_NUM", trim(col("BISMT")))\
        .withColumn("GRS_WT", col("BRGEW").cast(DecimalType(18, 4)))\
        .withColumn("ORDR_UNIT_PUR_UOM", trim(col("BSTME")))\
        .withColumn("CRT_BY", trim(col("ERNAM")))\
        .withColumn(
          "CRT_ON_DTTM",
          when((col("ERSDA") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("ERSDA"), "yyyyMMdd"))
        )\
        .withColumn("LBL_TYPE", trim(col("ETIAR")))\
        .withColumn("LBL_FORM", trim(col("ETIFO")))\
        .withColumn("EXTRNL_MATL_GRP", trim(col("EXTWG")))\
        .withColumn("MAX_LVL", trim(col("FUELG")))\
        .withColumn("WT_UNIT", trim(col("GEWEI")))\
        .withColumn("SIZE_DIM", trim(col("GROES")))\
        .withColumn("PER_IN", trim(col("IPRKZ")))\
        .withColumn(
          "LAST_CHG_DT_TIME_DTTM",
          when((col("LAEDA") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("LAEDA"), "yyyyMMdd"))
        )\
        .withColumn("MATL_GRP_PKGNG_MATL", trim(col("MAGRV")))\
        .withColumn("STRG_PCT", trim(col("MHDLP")))\
        .withColumn(
          "VAL_FROM_XPLNT_DTTM",
          when((col("MSTDE") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("MSTDE"), "yyyyMMdd"))
        )\
        .withColumn(
          "VAL_FROM_XDC_DTTM",
          when((col("MSTDV") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("MSTDV"), "yyyyMMdd"))
        )\
        .withColumn("INDSTR_STD_DESC", trim(col("NORMT")))\
        .withColumn("RD_RUL_SLED", trim(col("RDMHD")))\
        .withColumn("SER_LVL", trim(col("SERLV")))\
        .withColumn("MATL_HAZ_CD", trim(col("STOFF")))\
        .withColumn("VAR_ORDR_UNT", trim(col("VABME")))\
        .withColumn("PKGNG_MATL_TYPE", trim(col("VHART")))\
        .withColumn("VOL_UNIT", trim(col("VOLEH")))\
        .withColumn("VOL", col("VOLUM").cast(DecimalType(18, 4)))\
        .withColumn("BSC_MATL", trim(col("WRKST")))\
        .withColumn("DOC_TYPE", trim(col("ZEIAR")))\
        .withColumn("DOC_PG_FMT", trim(col("ZEIFO")))\
        .withColumn("EAN_UPC", trim(col("EAN11")))\
        .withColumn("EAN_CAT", trim(col("NUMTP")))\
        .withColumn("GTIN_VRNT", trim(col("GTIN_VARIANT")))\
        .withColumn(
          "EAN_UPC_HRMZD",
          when((length(expr("replace(trim(EAN_UPC), ' ', '')")) == lit(0)), lit(""))\
            .otherwise(concat(
            expr("substring('00000000000000', 1, (14 - length(replace(trim(EAN_UPC), ' ', ''))))"), 
            expr("replace(trim(EAN_UPC), ' ', '')")
          ))
        )\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_pk_L1", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_L1", lit("F"))\
        .withColumn("MMS_SURGERY_TYPE_CD", trim(col("zzmmssurgtype")))\
        .withColumn("MMS_MATL_TYPE_CD", trim(col("zzmmstype")))\
        .withColumn("PRMRY_PLNT_CD", trim(col("zzwerks")))\
        .withColumn("MMS_FIN_CLSN_CD", trim(col("zzmmsficlass")))\
        .withColumn("MMS_STERILIZATION_IND", trim(col("zzmmsterile")))\
        .withColumn("MATL_CATLG_NUM", trim(col("zzcatnumber")))\
        .withColumn("SRC_SECTR_CD", trim(col("zzsector")))\
        .withColumn("MATL_PARNT_CD", trim(col("zzp2_basecode")))\
        .withColumn("MATL_SUB_TYPE_CD", trim(col("zzmatsub_type")))\
        .withColumn("FIN_HIER_BASE_CD", trim(col("zzsec_prdgrp")))\
        .withColumn("IMPLNT_INSTM_IND", trim(col("zzprod_cat1")))\
        .withColumn("MATL_MOD_CD", trim(col("zzvariant")))\
        .withColumn("KIT_IND", trim(col("zzkit_ind")))\
        .withColumn("MMS_TEMP_SENS_IND", trim(col("zzmmsts")))\
        .withColumn("PLNG_HIER3_CD", trim(col("zzp3_low_level")))\
        .withColumn("MATL_TYPE_DESC", trim(lookup("T134T_LU", col("MTART")).getField("MTBEZ")))\
        .withColumn("MATL_GRP_DESC", trim(lookup("WGBEZx_LU", col("MATKL")).getField("WGBEZ")))\
        .withColumn("MATL_GRP_DESC_2", trim(lookup("WGBEZx_LU", col("MATKL")).getField("WGBEZ60")))\
        .withColumn(
        "SHRT_MATL_NUM",
        lit(
          "#"
        )
    )
