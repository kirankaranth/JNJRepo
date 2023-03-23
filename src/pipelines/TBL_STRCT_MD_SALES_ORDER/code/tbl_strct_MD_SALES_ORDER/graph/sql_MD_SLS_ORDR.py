from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_SALES_ORDER.config.ConfigStore import *
from tbl_strct_MD_SALES_ORDER.udfs.UDFs import *

def sql_MD_SLS_ORDR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as COMPANY_CD,\ncast('' as string) as SLS_ORDR_DOC_ID,\ncast('' as string) as SLS_ORDR_TYPE_CD,\ncast('' as string) as SLS_ORDR_TYPE_DESC,\ncast('' as string) as SLS_ORDR_NUM_REF,\ncast('' as string) as CRT_BY,\ncast('' as string) as BILL_BLK_CD,\ncast('' as string) as BILL_BLK_DESC,\ncast('' as string) as DELV_BLK_CD,\ncast('' as string) as DELV_BLK_DESC,\ncast('' as string) as CR_CHK_TOT_STS_CD,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as string) as CUST_PO_TYPE_CD,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as VALID_FROM_DTTM,\ncast('' as timestamp) as VALID_TO_DTTM,\ncast('' as timestamp) as RQST_DELV_DTTM,\ncast('' as string) as PRC_PCDR_CD,\ncast('' as string) as SLS_ORDR_CRNCY_CD,\ncast('' as decimal(18,4)) as NET_VAL_AMT,\ncast('' as string) as SOLD_TO_CUST_NUM,\ncast('' as string) as SALES_ORGANIZATION_CD,\ncast('' as string) as SLORG_NM,\ncast('' as string) as DSTR_CHNL_CD,\ncast('' as string) as DSTR_CHNL_NM,\ncast('' as string) as DIVISION_CD,\ncast('' as string) as DIV_NM,\ncast('' as string) as SHIPPING_COND_CD,\ncast('' as string) as SHIPPING_COND_DESC,\ncast('' as string) as CUST_PO_NUM,\ncast('' as string) as PTNR_REF_CD,\ncast('' as string) as SLS_ORDR_CAT_CD,\ncast('' as string) as SALES_ORDER_REASON_CD,\ncast('' as string) as SLS_ORDR_RSN_DESC,\ncast('' as string) as SLS_TERR_ID,\ncast('' as string) as SLS_GRP_CD,\ncast('' as timestamp) as ORIG_MATL_AVLBLTY_DTTM,\ncast('' as string) as REJ_TOT_STS_CD,\ncast('' as string) as CNFRM_STS_CD,\ncast('' as string) as PSTNG_BILL_STS_CD,\ncast('' as string) as INTCO_BILL_TOT_STS_CD,\ncast('' as string) as ORDR_BILL_STS_CD,\ncast('' as string) as BILL_STS_CD,\ncast('' as string) as PRCSG_TOT_STS_CD,\ncast('' as string) as PICK_CNFRM_STS_CD,\ncast('' as string) as PICK_TOT_STS_CD,\ncast('' as string) as DELV_STS_CD,\ncast('' as string) as DELV_TOT_STS_CD,\ncast('' as string) as WM_TOT_STS_CD,\ncast('' as string) as PACK_TOT_STS_CD,\ncast('' as string) as INVC_LIST_STS_CD,\ncast('' as string) as REF_DOC_TOT_STS_CD,\ncast('' as string) as REF_DOC_STS_CD,\ncast('' as string) as TRNSP_PLAN_STS_CD,\ncast('' as string) as ICMPT_TOT_STS_CD,\ncast('' as string) as BILL_ICMPT_TOT_STS_CD,\ncast('' as string) as PACKICMPT_STS_CD,\ncast('' as string) as PACKICMPT_TOT_STS_CD,\ncast('' as string) as PICKICMPT_STS_CD,\ncast('' as string) as PICKICMPT_TOT_STS_CD,\ncast('' as string) as PRCICMPT_STS_CD,\ncast('' as string) as DELVICMPT_TOT_STS_CD,\ncast('' as string) as GMICMPT_TOT_STS_CD,\ncast('' as string) as GM_TOT_STS_CD,\ncast('' as string) as DELV_BLK_STS_CD,\ncast('' as string) as CMPLT_DELV_CD,\ncast('' as timestamp) as RLSE_DTTM,\ncast('' as string) as RESV_CD,\ncast('' as string) as OVRL_HDR_CD,\ncast('' as string) as BILL_BLK_STS_CD,\ncast('' as string) as OVRL_BLK_STS_CD,\ncast('' as string) as PO_TYPE_DESC,\ncast('' as string) as SRCH_ITM_PROD_PROPS,\ncast('' as string) as RETRO_BILL,\ncast('' as timestamp) as DOC_DTTM,\ncast('' as string) as BUSN_AREA,\ncast('' as string) as BUSN_AREA_CC,\ncast('' as string) as SD_DOC_IN,\ncast('' as string) as ORDR_RLTD_BILL_TYPE,\ncast('' as string) as CC_CD,\ncast('' as string) as UPDT_GRP,\ncast('' as string) as CUST_GRP_1,\ncast('' as string) as CUST_GRP_1_DESC,\ncast('' as string) as CUST_GRP_2,\ncast('' as string) as CUST_GRP_2_DESC,\ncast('' as string) as CUST_GRP_3,\ncast('' as string) as CUST_GRP_3_DESC,\ncast('' as string) as CUST_GRP_4,\ncast('' as string) as CUST_GRP_4_DESC,\ncast('' as string) as CUST_GRP_5,\ncast('' as string) as CUST_GRP_5_DESC,\ncast('' as string) as CNTL_AREA,\ncast('' as string) as EXCH_RT_TYPE,\ncast('' as string) as CR_CNTL_AREA,\ncast('' as string) as CR_MGMT_RISK_CAT,\ncast('' as string) as ALT_TAX_CLSN,\ncast('' as string) as TAX_CLSN_2,\ncast('' as string) as TAX_CLSN_3,\ncast('' as string) as TAX_CLSN_4,\ncast('' as string) as TAX_CLSN_5,\ncast('' as string) as TAX_CLSN_6,\ncast('' as string) as TAX_CLSN_7,\ncast('' as string) as TAX_CLSN_8,\ncast('' as string) as TAX_CLSN_9,\ncast('' as string) as REF_DOC_NUM,\ncast('' as string) as ORDR_NUM,\ncast('' as string) as TAX_DEST_CTRY,\ncast('' as string) as TAX_DPRT_CTRY,\ncast('' as string) as IN_TRNGLR_DEAL_EU,\ncast('' as string) as CO_CD_DESC,\ncast('' as string) as CLS_OF_TRD,\ncast('' as string) as FORBID_SLS_IN,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1