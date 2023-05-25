from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_atl.config.ConfigStore import *
from sap_01_md_sls_ordr_atl.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SLS_ORDR_DOC_ID_VBUK"), 
        col("CR_CHK_TOT_STS_CD"), 
        col("REJ_TOT_STS_CD"), 
        col("CNFRM_STS_CD"), 
        col("PSTNG_BILL_STS_CD"), 
        col("INTCO_BILL_TOT_STS_CD"), 
        col("ORDR_BILL_STS_CD"), 
        col("BILL_STS_CD"), 
        col("PRCSG_TOT_STS_CD"), 
        col("PICK_CNFRM_STS_CD"), 
        col("PICK_TOT_STS_CD"), 
        col("DELV_STS_CD"), 
        col("DELV_TOT_STS_CD"), 
        col("WM_TOT_STS_CD"), 
        col("PACK_TOT_STS_CD"), 
        col("INVC_LIST_STS_CD"), 
        col("REF_DOC_TOT_STS_CD"), 
        col("REF_DOC_STS_CD"), 
        col("TRNSP_PLAN_STS_CD"), 
        col("ICMPT_TOT_STS_CD"), 
        col("BILL_ICMPT_TOT_STS_CD"), 
        col("PACKICMPT_STS_CD"), 
        col("PACKICMPT_TOT_STS_CD"), 
        col("PICKICMPT_STS_CD"), 
        col("PICKICMPT_TOT_STS_CD"), 
        col("PRCICMPT_STS_CD"), 
        col("DELVICMPT_TOT_STS_CD"), 
        col("GMICMPT_TOT_STS_CD"), 
        col("GM_TOT_STS_CD"), 
        col("DELV_BLK_STS_CD"), 
        col("RESV_CD"), 
        col("OVRL_HDR_CD"), 
        col("BILL_BLK_STS_CD"), 
        col("OVRL_BLK_STS_CD")
    )
