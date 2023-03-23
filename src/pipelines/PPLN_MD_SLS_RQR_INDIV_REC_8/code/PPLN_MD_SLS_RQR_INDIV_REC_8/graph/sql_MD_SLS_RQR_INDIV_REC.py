from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MD_SLS_RQR_INDIV_REC_8.config.ConfigStore import *
from PPLN_MD_SLS_RQR_INDIV_REC_8.udfs.UDFs import *

def sql_MD_SLS_RQR_INDIV_REC(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
 '{Config.sourceSystem}'   AS SRC_SYS_CD,
    VBBE.VBELN  AS SLS_DOC,
    VBBE.POSNR  AS SLS_DOC_ITM,
    VBBE.ETENR  AS SCHED_LINE_NUM,
    trim(VBBE.MATNR)  AS MATL_NUM,
    trim(VBBE.WERKS)  AS PLNT,
    trim(VBBE.BERID)  AS MRP_AREA,
    case when VBBE.MBDAT = '00000000' then CAST(null as TIMESTAMP) else to_timestamp(VBBE.MBDAT, \"yyyyMMdd\") end AS MATL_STGNG_AVLBLTY_DTTM,
    trim(VBBE.LGORT)  AS STRG_LOC,
    trim(VBBE.CHARG)  AS BTCH_NUM,
    trim(VBBE.VBTYP)  AS SD_DOC_CAT,
    trim(VBBE.BDART)  AS REQ_TYPE,
    trim(VBBE.PLART)  AS PLNG_TYPE,
    CAST(trim(VBBE.OMENG) AS DECIMAL(18,4))  AS OPEN_QTY_SKU_TFR_RQR_MRP,
    CAST(trim(VBBE.VMENG) AS DECIMAL(18,4))  AS CNFRM_QTY_AVLBLTY_CHK_SKU,
    trim(VBBE.MEINS)  AS BASE_UNIT_MEAS,
    trim(VBBE.VBELE)  AS BUSN_DOC_NUM,
    trim(VBBE.POSNE)  AS BUSN_ITM_NUM,
    trim(VBBE.ETENE)  AS SCHED_LINE,
    trim(VBBE.AWAHR)  AS ORDR_PRBLTY_ITM,
    trim(VBBE.AUART)  AS SLS_DOC_TYPE,
    trim(VBBE.PROJN)  AS OLD_PROJ_NUM_NO_LONG_USED_PS_POSNR,
    trim(VBBE.KUNNR)  AS SOLD_TO_PRTY,
    trim(VBBE.NODIS)  AS RQR_REC_NOT_RLVNT_MRP,
    trim(VBBE.VPZUO)  AS ALLC_IN,
    trim(VBBE.VPMAT)  AS PLNG_MATL,
    trim(VBBE.VPWRK)  AS PLNG_PLNT,
    trim(VBBE.PRBME)  AS BASE_UNIT_MEAS_PROD_GRP,
    CAST(trim(VBBE.UMREF) AS DECIMAL(18,4))  AS CONV_FACT_QTY,
    CAST(trim(VBBE.PZMNG) AS DECIMAL(18,4)) AS PLAN_ALLC_QTY_INDP_RQR,
    trim(VBBE.KNTTP)  AS ACCT_ASGNMT_CAT,
    trim(VBBE.SOBKZ)  AS SPL_STK_IN,
    trim(VBBE.KZVBR)  AS CNSMPTN_PSTNG,
    trim(VBBE.SERNR)  AS BOM_EXPLS_NUM,
    trim(VBBE.PSPEL)  AS WBS_ELMNT,
    trim(VBBE.PLNKZ)  AS PLNG_IN,
    trim(VBBE.CUOBJ)  AS CNFG,
    trim(VBBE.MONKZ)  AS IN_ASBL_ORDR_PCDR,
    trim(VBBE.KZBWS)  AS VALUT_SPL_STK,
    trim(VBBE.TECHS)  AS PARM_VRNT_STD_VRNT,
    NULL AS REQ_SGMNT,
    CAST(NULL AS DECIMAL(18,4)) AS ARUN_REQ_ALC_QTY
FROM
{Config.sourceDatabase}.VBBE VBBE
WHERE VBBE._deleted_ = 'F'
AND VBBE.MANDT = '050'  
 
"""
    )

    return out0
