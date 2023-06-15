from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_atl.config.ConfigStore import *
from sap_01_md_sls_ordr_atl.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("COMPANY_CD"), 
        col("SLS_ORDR_DOC_ID"), 
        col("SLS_ORDR_TYPE_CD"), 
        col("CRT_DTTM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_"), 
        col("SLS_ORDR_NUM_REF"), 
        col("CRT_BY"), 
        col("BILL_BLK_CD"), 
        col("DELV_BLK_CD"), 
        col("CHG_DTTM"), 
        col("CUST_PO_TYPE_CD"), 
        col("VALID_FROM_DTTM"), 
        col("VALID_TO_DTTM"), 
        col("RQST_DELV_DTTM"), 
        col("PRC_PCDR_CD"), 
        col("SLS_ORDR_CRNCY_CD"), 
        col("NET_VAL_AMT"), 
        col("SOLD_TO_CUST_NUM"), 
        col("SALES_ORGANIZATION_CD"), 
        col("DSTR_CHNL_CD"), 
        col("DIVISION_CD"), 
        col("SHIPPING_COND_CD"), 
        col("CUST_PO_NUM"), 
        col("PTNR_REF_CD"), 
        col("SLS_ORDR_CAT_CD"), 
        col("SALES_ORDER_REASON_CD"), 
        col("SLS_TERR_ID"), 
        col("SLS_GRP_CD"), 
        col("ORIG_MATL_AVLBLTY_DTTM"), 
        col("CMPLT_DELV_CD"), 
        col("RLSE_DTTM"), 
        col("SRCH_ITM_PROD_PROPS"), 
        col("DOC_DTTM"), 
        col("BUSN_AREA"), 
        col("BUSN_AREA_CC"), 
        col("SD_DOC_IN"), 
        col("ORDR_RLTD_BILL_TYPE"), 
        col("CC_CD"), 
        col("UPDT_GRP"), 
        col("CUST_GRP_1"), 
        col("CUST_GRP_2"), 
        col("CUST_GRP_3"), 
        col("CUST_GRP_4"), 
        col("CUST_GRP_5"), 
        col("CNTL_AREA"), 
        col("EXCH_RT_TYPE"), 
        col("CR_CNTL_AREA"), 
        col("CR_MGMT_RISK_CAT"), 
        col("ALT_TAX_CLSN"), 
        col("TAX_CLSN_2"), 
        col("TAX_CLSN_3"), 
        col("TAX_CLSN_4"), 
        col("TAX_CLSN_5"), 
        col("TAX_CLSN_6"), 
        col("TAX_CLSN_7"), 
        col("TAX_CLSN_8"), 
        col("TAX_CLSN_9"), 
        col("REF_DOC_NUM"), 
        col("ORDR_NUM"), 
        col("TAX_DEST_CTRY"), 
        col("TAX_DPRT_CTRY"), 
        col("IN_TRNGLR_DEAL_EU"), 
        col("FORBID_SLS_IN"), 
        col("SLS_ORDR_TYPE_DESC"), 
        col("BILL_BLK_DESC"), 
        col("DELV_BLK_DESC"), 
        col("SLORG_NM"), 
        col("DSTR_CHNL_NM"), 
        col("DIV_NM"), 
        col("SLS_ORDR_RSN_DESC"), 
        col("PO_TYPE_DESC"), 
        col("RETRO_BILL"), 
        col("CUST_GRP_2_DESC"), 
        col("CUST_GRP_3_DESC"), 
        col("CUST_GRP_4_DESC"), 
        col("CUST_GRP_5_DESC"), 
        col("CO_CD_DESC"), 
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
