from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_DELIVERIES.config.ConfigStore import *
from tbl_strct_MD_DELIVERIES.udfs.UDFs import *

def sql_MD_DELV(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as CO_CD,\ncast('' as string) as DELV_NUM,\ncast('' as string) as DELV_TYP_CD,\ncast('' as string) as DELV_TYP_DESC,\ncast('' as string) as BILL_OF_LDNG_NUM,\ncast('' as string) as BILL_ICMPT_TOT_STS_CD,\ncast('' as string) as BILL_STS_CD,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as string) as CNFRM_STS_CD,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as string) as CR_CHK_TOT_STS_CD,\ncast('' as timestamp) as DELV_DTTM,\ncast('' as string) as SHIP_STS_CD,\ncast('' as string) as SPL_PRCS_IN,\ncast('' as string) as SLS_ORG_NUM,\ncast('' as string) as SLS_ORG_NM,\ncast('' as string) as DELV_ICMPT_TOT_STS_CD,\ncast('' as string) as DELV_STS_CD,\ncast('' as string) as DELV_TOT_STS_CD,\ncast('' as string) as RTE_ID,\ncast('' as string) as DELIVERY_NUM,\ncast('' as string) as REF_DOC_NUM,\ncast('' as string) as GM_ICMPT_TOT_STS_CD,\ncast('' as string) as GM_TOT_STS_CD,\ncast('' as string) as IN_PLNT_IND,\ncast('' as string) as ICMPT_TOT_STS_CD,\ncast('' as string) as INTCO_BILL_TOT_STS_CD,\ncast('' as string) as INVC_LIST_STS_CD,\ncast('' as string) as ORDR_BILL_STS_CD,\ncast('' as string) as PACK_ICMPT_STS_CD,\ncast('' as string) as PACK_ICMPT_TOT_STS_CD,\ncast('' as string) as PACK_TOT_STS_CD,\ncast('' as string) as PICK_CNFRM_STS_CD,\ncast('' as string) as PICK_ICMPT_STS_CD,\ncast('' as string) as PICK_ICMPT_TOT_STS_CD,\ncast('' as string) as PICK_TOT_STS_CD,\ncast('' as timestamp) as PLAN_GI_DTTM,\ncast('' as timestamp) as ACTL_GI_DTTM,\ncast('' as string) as PLNT_CD,\ncast('' as string) as PSTNG_BILL_STS_CD,\ncast('' as string) as PRC_ICMPT_STS_CD,\ncast('' as string) as PRCS_ORDR_NUM,\ncast('' as string) as PRCSG_TOT_STS_CD,\ncast('' as string) as REF_DOC_STS_CD,\ncast('' as string) as REF_DOC_TOT_STS_CD,\ncast('' as string) as REJ_TOT_STS_CD,\ncast('' as string) as SLS_ORDR_CAR_CD,\ncast('' as string) as SHIP_TO_CUST_NUM,\ncast('' as string) as SHIPPING_COND_CD,\ncast('' as string) as SHIPPING_PT_NUM,\ncast('' as string) as SOLD_TO_CUST_NUM,\ncast('' as string) as SUP_NUM,\ncast('' as int) as TOT_PKGS_CNT,\ncast('' as string) as TRNSP_PLAN_STS_CD,\ncast('' as string) as WM_TOT_STS_CD,\ncast('' as timestamp) as PICK_DTTM,\ncast('' as string) as SLS_ORDR_CRNCY_CD,\ncast('' as string) as RESV_CD,\ncast('' as string) as OVRL_HDR_CD,\ncast('' as string) as BILL_BLK_STS_CD,\ncast('' as string) as DELV_BLK_STS_CD,\ncast('' as string) as OVRL_BLK_STS_CD,\ncast('' as timestamp) as POD_DTTM,\ncast('' as decimal (18,4)) as TOT_WT_CD,\ncast('' as string) as GR_SLIP_NUM,\ncast('' as string) as MEANS_TRNSP_ID,\ncast('' as string) as SRC_TBL_NM,\ncast('' as string) as SHIP_REF_NUM,\ncast('' as string) as MODE_TRSPN_BORDR,\ncast('' as string) as DFLT_DELV_BLK_CD,\ncast('' as string) as DFLT_DELV_BLK_DESC,\ncast('' as string) as CNFRM_BLK_CD,\ncast('' as string) as DELV_BLK_CD,\ncast('' as string) as SHIPPING_PT_DESC,\ncast('' as string) as SHIP_BLOK_RSN_CD,\ncast('' as string) as SHIP_BLOK_RSN_DESC,\ncast('' as string) as FCTRY_CAL_KEY,\ncast('' as string) as RTE_DESC,\ncast('' as decimal (18,4)) as TRSPN_LEAD_TIME_IN_CAL_DAYS,\ncast('' as decimal (18,4)) as TRST_DUR_IN_CAL_DAYS,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1