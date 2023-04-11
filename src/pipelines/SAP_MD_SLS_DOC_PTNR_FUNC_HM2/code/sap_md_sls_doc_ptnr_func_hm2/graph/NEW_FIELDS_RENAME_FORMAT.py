from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sls_doc_ptnr_func_hm2.config.ConfigStore import *
from sap_md_sls_doc_ptnr_func_hm2.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SLS_DOC_ITEM_NBR", col("POSNR"))\
        .withColumn("SLS_DOC_ID", col("VBELN"))\
        .withColumn("PTNR_FUNC_CD", col("PARVW"))\
        .withColumn("COMPANY_CD", col("BUKRS_VF"))\
        .withColumn("SLS_ORDR_TYPE_CD", col("AUART"))\
        .withColumn("FURTHER_PTNR_IND", trim(col("PARVW_FF")))\
        .withColumn("SUP_NUM", trim(col("LIFNR")))\
        .withColumn("ADDR_USG_CD", trim(col("ADRNR")))\
        .withColumn("CUST_NUM", trim(col("KUNNR")))\
        .withColumn("CTRY_CD", trim(col("LAND1")))\
        .withColumn("PERS_NUM", trim(col("PERNR")))\
        .withColumn("NUM_CNTCT_PRSN", trim(col("PARNR")))\
        .withColumn("UNLD_PT", trim(col("ABLAD")))\
        .withColumn("ADDR_IN", trim(col("ADRDA")))\
        .withColumn("IN_ACCT_ONE_TM_ACCT", trim(col("XCPDK")))\
        .withColumn("CUST_HIER_TYPE", trim(col("HITYP")))\
        .withColumn("RLVNT_PRC_DTRMN_ID", trim(col("PRFRE")))\
        .withColumn("IN_CUST_REBT_RLVNT", trim(col("BOKRE")))\
        .withColumn("LVL_NUM_WTHN_HIER", trim(col("HISTUNR")))\
        .withColumn("CUST_DESC_PTNR", trim(col("KNREF")))\
        .withColumn("TRSPN_ZN_GOODS_DELV", trim(col("LZONE")))\
        .withColumn("ASGNMT_HIER", trim(col("HZUOR")))\
        .withColumn("VAT_REGS_NUM", trim(col("STCEG")))\
        .withColumn("PRSN_NUM", trim(col("ADRNP")))\
        .withColumn("MANTN_APPT_CAL", trim(col("KALE")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SLS_DOC_ITEM_NBR', SLS_DOC_ITEM_NBR, 'SLS_DOC_ID', SLS_DOC_ID, 'PTNR_FUNC_CD', PTNR_FUNC_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SLS_DOC_ITEM_NBR', SLS_DOC_ITEM_NBR, 'SLS_DOC_ID', SLS_DOC_ID, 'PTNR_FUNC_CD', PTNR_FUNC_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
              )
            )
          )
        )\
        .withColumn("_deleted_", lit("F"))\
        .withColumn(
          "DATA_FIL_VAL_DATA_AGE_DTTM",
          when((col("_DATAAGING") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(col("_DATAAGING"), "yyyyMMdd"))\
            .alias("DATA_FIL_VAL_DATA_AGE_DTTM")
        )\
        .withColumn("BUSN_PTNR_NUM", trim(col("ASSIGNED_BP")))\
        .withColumn("ADDR_DTRMN_DOC", trim(col("ADDR_OPERATION")))\
        .withColumn("ADDR_TYPE", trim(col("ADDR_TYPE")))\
        .withColumn("BP_REF_ADDR_NUM", trim(col("BP_REF_ADRNR")))\
        .withColumn("DUMMY_FUNC_LGTH_1", trim(col("DUMMY_SDDOCPARTNER_INCL_EEW_PS")))\
        .withColumn("SDM_VERS_FLD_FOR_VBPA", trim(col("SDM_VERSION")))
