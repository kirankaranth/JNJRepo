from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_PRCH_INFO_ORG_1.config.ConfigStore import *
from MD_PRCH_INFO_ORG_1.udfs.UDFs import *

def sql_MD_PRCH_INFO_ORG(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT '{Config.sourceSystem}' AS SRC_SYS_CD,
EINE.infnr  AS  PRCH_INFO_NUM,
EINE.ekorg  AS  PRCHSNG_ORG_NUM,
EINE.esokz  AS  PRCH_INFO_CAT_CD,
EINE.werks  AS  PLNT_CD,
TRIM(EINE.loekz)  AS  DEL_IND,
CASE WHEN EINE.erdat= '00000000' THEN NULL ELSE TO_TIMESTAMP(EINE.erdat, \"yyyyMMdd\") END  AS  CRT_ON_DTTM,
TRIM(EINE.ernam)  AS  CRT_BY_NM,
TRIM(EINE.ekgrp)  AS  PRCHSNG_GRP_NUM,
TRIM(EINE.waers)  AS  CRNCY_CD,
CAST(TRIM(EINE.minbm) AS DECIMAL(18,4))  AS  MIN_PO_QTY,
CAST(TRIM(EINE.norbm) AS DECIMAL(18,4))  AS  STD_PO_QTY,
TRIM(EINE.bonus)  AS  VOL_REBT_IND,
TRIM(EINE.mgbon)  AS  QTY_REBT,
CAST(TRIM(EINE.aplfz) AS DECIMAL(18,4)) AS  PLAN_DELV_DAYS,
TRIM(EINE.mwskz)  AS  TAX_CD,
TRIM(EINE.bstae)  AS  CNFRM_CAT_CD,
TRIM(EINE.meprf)  AS  PRC_CNTL_IND,
TRIM(EINE.inco1)  AS  INCOTERM_1_CD,
TRIM(EINE.inco2)  AS  INCOTERM_2_CD,
TRIM(EINE.verid)  AS  PRDTN_VERS_NUM,
CAST(TRIM(EINE.bstma) AS DECIMAL(18,4))  AS  MAX_PO_QTY,
TRIM(EINE.rdprf)  AS  RD_PRFL_CD,
TRIM(EINE.j_1bnbm)  AS  BRAZLIAN_NCM_CD,
EINE._upt_ as _l0_upt_
FROM
{Config.sourceDatabase}.EINE EINE
WHERE EINE._deleted_ = 'F'
AND EINE.MANDT = {Config.MANDT}
"""
    )

    return out0
