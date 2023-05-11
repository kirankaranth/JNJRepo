from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.config.ConfigStore import *
from jde_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.udfs.UDFs import *

def sql_MD_DELV(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    with mysub
as
(select
F43121.PRKCOO AS CO_CD,
F43121.PRDOC AS DELV_NUM,
F43121.PRDCTO AS DELV_TYP_CD,
NULL AS DELV_TYP_DESC,
NULL AS BILL_OF_LDNG_NUM,
NULL AS BILL_ICMPT_TOT_STS_CD,
NULL AS BILL_STS_CD,
NULL AS CHG_DTTM,
NULL AS CNFRM_STS_CD,
NULL AS CRT_DTTM,
NULL AS CR_CHK_TOT_STS_CD,
NULL AS DELV_DTTM,
NULL AS SHIP_STS_CD,
NULL AS SPL_PRCS_IN,
NULL AS SLS_ORG_NUM,
NULL AS SLS_ORG_NM,
NULL AS DELV_ICMPT_TOT_STS_CD,
NULL AS DELV_STS_CD,
NULL AS DELV_TOT_STS_CD,
NULL AS RTE_ID,
NULL AS DELIVERY_NUM,
F43121.PRDOCO AS REF_DOC_NUM,
NULL AS GM_ICMPT_TOT_STS_CD,
NULL AS GM_TOT_STS_CD,
NULL AS IN_PLNT_IND,
NULL AS ICMPT_TOT_STS_CD,
NULL AS INTCO_BILL_TOT_STS_CD,
NULL AS INVC_LIST_STS_CD,
NULL AS ORDR_BILL_STS_CD,
NULL AS PACK_ICMPT_STS_CD,
NULL AS PACK_ICMPT_TOT_STS_CD,
NULL AS PACK_TOT_STS_CD,
NULL AS PICK_CNFRM_STS_CD,
NULL AS PICK_ICMPT_STS_CD,
NULL AS PICK_ICMPT_TOT_STS_CD,
NULL AS PICK_TOT_STS_CD,
NULL as PLAN_GI_DTTM,
NULL AS ACTL_GI_DTTM,
TRIM(F43121.PRMCU) AS PLNT_CD,
NULL AS PSTNG_BILL_STS_CD,
NULL AS PRC_ICMPT_STS_CD,
NULL AS PRCS_ORDR_NUM,
NULL AS PRCSG_TOT_STS_CD,
NULL AS REF_DOC_STS_CD,
NULL AS REF_DOC_TOT_STS_CD,
NULL AS REJ_TOT_STS_CD,
NULL AS SLS_ORDR_CAR_CD,
NULL AS SHIP_TO_CUST_NUM,
NULL AS SHIPPING_COND_CD,
NULL AS SHIPPING_PT_NUM,
NULL AS SOLD_TO_CUST_NUM,
NULL AS SUP_NUM,
NULL AS TOT_PKGS_CNT,
NULL AS TRNSP_PLAN_STS_CD,
NULL AS WM_TOT_STS_CD,
NULL AS PICK_DTTM,
NULL AS SLS_ORDR_CRNCY_CD,
NULL AS RESV_CD,
NULL AS OVRL_HDR_CD,
NULL AS BILL_BLK_STS_CD,
NULL AS DELV_BLK_STS_CD,
NULL AS OVRL_BLK_STS_CD,
NULL AS POD_DTTM,
NULL AS TOT_WT_CD,
NULL AS GR_SLIP_NUM,
NULL AS MEANS_TRNSP_ID,
'F43121' AS SRC_TBL_NM,
CAST(NULL AS string) as SHIP_REF_NUM,
CAST(NULL AS string) as MODE_TRSPN_BORDR,
CAST(NULL AS string) as DFLT_DELV_BLK_CD,
CAST(NULL AS string) as DFLT_DELV_BLK_DESC,
CAST(NULL AS string) as CNFRM_BLK_CD,
CAST(NULL AS string) as DELV_BLK_CD,
CAST(NULL AS string) as SHIPPING_PT_DESC,
CAST(NULL AS string) as SHIP_BLOK_RSN_CD,
CAST(NULL AS string) as SHIP_BLOK_RSN_DESC,
CAST(NULL AS string) as FCTRY_CAL_KEY,
CAST(NULL AS string) as RTE_DESC,
CAST(NULL AS decimal (18,4)) as TRSPN_LEAD_TIME_IN_CAL_DAYS,
CAST(NULL AS decimal (18,4)) as TRST_DUR_IN_CAL_DAYS,
F43121._upt_ as _l0_upt_
from {Config.sourceDatabase}.F43121 F43121
where F43121._deleted_ = 'F'
UNION ALL
SELECT
F4211.SDKCOO AS CO_CD,
F4211.SDDOC AS DELV_NUM,
F4211.SDDCTO AS DELV_TYP_CD,
NULL AS DELV_TYP_DESC,
NULL AS BILL_OF_LDNG_NUM,
NULL AS BILL_ICMPT_TOT_STS_CD,
NULL AS BILL_STS_CD,
NULL AS CHG_DTTM,
NULL AS CNFRM_STS_CD,
NULL AS CRT_DTTM,
NULL AS CR_CHK_TOT_STS_CD,
NULL AS DELV_DTTM,
NULL AS SHIP_STS_CD,
NULL AS SPL_PRCS_IN,
NULL AS SLS_ORG_NUM,
NULL AS SLS_ORG_NM,
NULL AS DELV_ICMPT_TOT_STS_CD,
NULL AS DELV_STS_CD,
NULL AS DELV_TOT_STS_CD,
NULL AS RTE_ID,
NULL AS DELIVERY_NUM,
F4211.SDDOCO AS REF_DOC_NUM,
NULL AS GM_ICMPT_TOT_STS_CD,
NULL AS GM_TOT_STS_CD,
NULL AS IN_PLNT_IND,
NULL AS ICMPT_TOT_STS_CD,
NULL AS INTCO_BILL_TOT_STS_CD,
NULL AS INVC_LIST_STS_CD,
NULL AS ORDR_BILL_STS_CD,
NULL AS PACK_ICMPT_STS_CD,
NULL AS PACK_ICMPT_TOT_STS_CD,
NULL AS PACK_TOT_STS_CD,
NULL AS PICK_CNFRM_STS_CD,
NULL AS PICK_ICMPT_STS_CD,
NULL AS PICK_ICMPT_TOT_STS_CD,
NULL AS PICK_TOT_STS_CD,
NULL as PLAN_GI_DTTM,
NULL AS ACTL_GI_DTTM,
TRIM(F4211.SDMCU) AS PLNT_CD,
NULL AS PSTNG_BILL_STS_CD,
NULL AS PRC_ICMPT_STS_CD,
NULL AS PRCS_ORDR_NUM,
NULL AS PRCSG_TOT_STS_CD,
NULL AS REF_DOC_STS_CD,
NULL AS REF_DOC_TOT_STS_CD,
NULL AS REJ_TOT_STS_CD,
NULL AS SLS_ORDR_CAR_CD,
NULL AS SHIP_TO_CUST_NUM,
NULL AS SHIPPING_COND_CD,
NULL AS SHIPPING_PT_NUM,
NULL AS SOLD_TO_CUST_NUM,
NULL AS SUP_NUM,
NULL AS TOT_PKGS_CNT,
NULL AS TRNSP_PLAN_STS_CD,
NULL AS WM_TOT_STS_CD,
NULL AS PICK_DTTM,
NULL AS SLS_ORDR_CRNCY_CD,
NULL AS RESV_CD,
NULL AS OVRL_HDR_CD,
NULL AS BILL_BLK_STS_CD,
NULL AS DELV_BLK_STS_CD,
NULL AS OVRL_BLK_STS_CD,
NULL AS POD_DTTM,
NULL AS TOT_WT_CD,
NULL AS GR_SLIP_NUM,
NULL AS MEANS_TRNSP_ID,
'F4211' AS SRC_TBL_NM,
CAST(NULL AS string) as SHIP_REF_NUM,
CAST(NULL AS string) as MODE_TRSPN_BORDR,
CAST(NULL AS string) as DFLT_DELV_BLK_CD,
CAST(NULL AS string) as DFLT_DELV_BLK_DESC,
CAST(NULL AS string) as CNFRM_BLK_CD,
CAST(NULL AS string) as DELV_BLK_CD,
CAST(NULL AS string) as SHIPPING_PT_DESC,
CAST(NULL AS string) as SHIP_BLOK_RSN_CD,
CAST(NULL AS string) as SHIP_BLOK_RSN_DESC,
CAST(NULL AS string) as FCTRY_CAL_KEY,
CAST(NULL AS string) as RTE_DESC,
CAST(NULL AS decimal (18,4)) as TRSPN_LEAD_TIME_IN_CAL_DAYS,
CAST(NULL AS decimal (18,4)) as TRST_DUR_IN_CAL_DAYS,
F4211._upt_ as _l0_upt_
from {Config.sourceDatabase}.F4211 F4211
where F4211._deleted_ = 'F')
select 
SRC_SYS_CD,CO_CD,DELV_NUM,DELV_TYP_CD,DELV_TYP_DESC,BILL_OF_LDNG_NUM,BILL_ICMPT_TOT_STS_CD,BILL_STS_CD,CHG_DTTM,CNFRM_STS_CD,CRT_DTTM,CR_CHK_TOT_STS_CD,
DELV_DTTM,SHIP_STS_CD,SPL_PRCS_IN,SLS_ORG_NUM,SLS_ORG_NM,DELV_ICMPT_TOT_STS_CD,DELV_STS_CD,DELV_TOT_STS_CD,RTE_ID,DELIVERY_NUM,REF_DOC_NUM,
GM_ICMPT_TOT_STS_CD,GM_TOT_STS_CD,IN_PLNT_IND,ICMPT_TOT_STS_CD,INTCO_BILL_TOT_STS_CD,INVC_LIST_STS_CD,ORDR_BILL_STS_CD,PACK_ICMPT_STS_CD,PACK_ICMPT_TOT_STS_CD,
PACK_TOT_STS_CD,PICK_CNFRM_STS_CD,PICK_ICMPT_STS_CD,PICK_ICMPT_TOT_STS_CD,PICK_TOT_STS_CD,PLAN_GI_DTTM,ACTL_GI_DTTM,PLNT_CD,PSTNG_BILL_STS_CD,PRC_ICMPT_STS_CD,
PRCS_ORDR_NUM,PRCSG_TOT_STS_CD,REF_DOC_STS_CD,REF_DOC_TOT_STS_CD,REJ_TOT_STS_CD,SLS_ORDR_CAR_CD,SHIP_TO_CUST_NUM,SHIPPING_COND_CD,SHIPPING_PT_NUM,SOLD_TO_CUST_NUM,
SUP_NUM,TOT_PKGS_CNT,TRNSP_PLAN_STS_CD,WM_TOT_STS_CD,PICK_DTTM,SLS_ORDR_CRNCY_CD,RESV_CD,OVRL_HDR_CD,BILL_BLK_STS_CD,
DELV_BLK_STS_CD,OVRL_BLK_STS_CD,POD_DTTM,TOT_WT_CD,GR_SLIP_NUM,MEANS_TRNSP_ID,SRC_TBL_NM, SHIP_REF_NUM, MODE_TRSPN_BORDR, DFLT_DELV_BLK_CD, DFLT_DELV_BLK_DESC,
CNFRM_BLK_CD, DELV_BLK_CD, SHIPPING_PT_DESC, SHIP_BLOK_RSN_CD, SHIP_BLOK_RSN_DESC, FCTRY_CAL_KEY, RTE_DESC, TRSPN_LEAD_TIME_IN_CAL_DAYS, TRST_DUR_IN_CAL_DAYS, _l0_upt_

from
(select *,
ROW_NUMBER() over (partition by SRC_SYS_CD,CO_CD,DELV_NUM,DELV_TYP_CD,REF_DOC_NUM,SRC_TBL_NM order by DELV_NUM) as RNK
from 
(SELECT DISTINCT
'{Config.sourceSystem}' AS SRC_SYS_CD
,mysub.CO_CD AS CO_CD
,mysub.DELV_NUM AS DELV_NUM
,mysub.DELV_TYP_CD AS DELV_TYP_CD
,mysub.DELV_TYP_DESC AS DELV_TYP_DESC
,mysub.BILL_OF_LDNG_NUM AS BILL_OF_LDNG_NUM
,mysub.BILL_ICMPT_TOT_STS_CD AS BILL_ICMPT_TOT_STS_CD
,mysub.BILL_STS_CD AS BILL_STS_CD
,mysub.CHG_DTTM AS CHG_DTTM
,mysub.CNFRM_STS_CD AS CNFRM_STS_CD
,mysub.CRT_DTTM AS CRT_DTTM
,mysub.CR_CHK_TOT_STS_CD AS CR_CHK_TOT_STS_CD
,mysub.DELV_DTTM AS DELV_DTTM
,mysub.SHIP_STS_CD AS SHIP_STS_CD
,mysub.SPL_PRCS_IN AS SPL_PRCS_IN
,mysub.SLS_ORG_NUM AS SLS_ORG_NUM
,mysub.SLS_ORG_NM AS SLS_ORG_NM
,mysub.DELV_ICMPT_TOT_STS_CD AS DELV_ICMPT_TOT_STS_CD
,mysub.DELV_STS_CD AS DELV_STS_CD
,mysub.DELV_TOT_STS_CD AS DELV_TOT_STS_CD
,mysub.RTE_ID AS RTE_ID
,mysub.DELIVERY_NUM AS DELIVERY_NUM
,mysub.REF_DOC_NUM AS REF_DOC_NUM
,mysub.GM_ICMPT_TOT_STS_CD AS GM_ICMPT_TOT_STS_CD
,mysub.GM_TOT_STS_CD AS GM_TOT_STS_CD
,mysub.IN_PLNT_IND AS IN_PLNT_IND
,mysub.ICMPT_TOT_STS_CD AS ICMPT_TOT_STS_CD
,mysub.INTCO_BILL_TOT_STS_CD AS INTCO_BILL_TOT_STS_CD
,mysub.INVC_LIST_STS_CD AS INVC_LIST_STS_CD
,mysub.ORDR_BILL_STS_CD AS ORDR_BILL_STS_CD
,mysub.PACK_ICMPT_STS_CD AS PACK_ICMPT_STS_CD
,mysub.PACK_ICMPT_TOT_STS_CD AS PACK_ICMPT_TOT_STS_CD
,mysub.PACK_TOT_STS_CD AS PACK_TOT_STS_CD
,mysub.PICK_CNFRM_STS_CD AS PICK_CNFRM_STS_CD
,mysub.PICK_ICMPT_STS_CD AS PICK_ICMPT_STS_CD
,mysub.PICK_ICMPT_TOT_STS_CD AS PICK_ICMPT_TOT_STS_CD
,mysub.PICK_TOT_STS_CD AS PICK_TOT_STS_CD
,mysub.PLAN_GI_DTTM as PLAN_GI_DTTM
,mysub.ACTL_GI_DTTM AS ACTL_GI_DTTM
,mysub.PLNT_CD AS PLNT_CD
,mysub.PSTNG_BILL_STS_CD AS PSTNG_BILL_STS_CD
,mysub.PRC_ICMPT_STS_CD AS PRC_ICMPT_STS_CD
,mysub.PRCS_ORDR_NUM AS PRCS_ORDR_NUM
,mysub.PRCSG_TOT_STS_CD AS PRCSG_TOT_STS_CD
,mysub.REF_DOC_STS_CD AS REF_DOC_STS_CD
,mysub.REF_DOC_TOT_STS_CD AS REF_DOC_TOT_STS_CD
,mysub.REJ_TOT_STS_CD AS REJ_TOT_STS_CD
,mysub.SLS_ORDR_CAR_CD AS SLS_ORDR_CAR_CD
,mysub.SHIP_TO_CUST_NUM AS SHIP_TO_CUST_NUM
,mysub.SHIPPING_COND_CD AS SHIPPING_COND_CD
,mysub.SHIPPING_PT_NUM AS SHIPPING_PT_NUM
,mysub.SOLD_TO_CUST_NUM AS SOLD_TO_CUST_NUM
,mysub.SUP_NUM AS SUP_NUM
,mysub.TOT_PKGS_CNT AS TOT_PKGS_CNT
,mysub.TRNSP_PLAN_STS_CD AS TRNSP_PLAN_STS_CD
,mysub.WM_TOT_STS_CD AS WM_TOT_STS_CD
,mysub.PICK_DTTM AS PICK_DTTM
,mysub.SLS_ORDR_CRNCY_CD AS SLS_ORDR_CRNCY_CD
,mysub.RESV_CD AS RESV_CD
,mysub.OVRL_HDR_CD AS OVRL_HDR_CD
,mysub.BILL_BLK_STS_CD AS BILL_BLK_STS_CD
,mysub.DELV_BLK_STS_CD AS DELV_BLK_STS_CD
,mysub.OVRL_BLK_STS_CD AS OVRL_BLK_STS_CD
,mysub.POD_DTTM AS POD_DTTM
,mysub.TOT_WT_CD AS TOT_WT_CD
,mysub.GR_SLIP_NUM AS GR_SLIP_NUM
,mysub.MEANS_TRNSP_ID AS MEANS_TRNSP_ID
,mysub.SRC_TBL_NM AS SRC_TBL_NM
,mysub.SHIP_REF_NUM AS SHIP_REF_NUM
,mysub.MODE_TRSPN_BORDR AS MODE_TRSPN_BORDR
,mysub.DFLT_DELV_BLK_CD AS DFLT_DELV_BLK_CD
,mysub.DFLT_DELV_BLK_DESC AS DFLT_DELV_BLK_DESC
,mysub.CNFRM_BLK_CD AS CNFRM_BLK_CD
,mysub.DELV_BLK_CD AS DELV_BLK_CD
,mysub.SHIPPING_PT_DESC AS SHIPPING_PT_DESC
,mysub.SHIP_BLOK_RSN_CD AS SHIP_BLOK_RSN_CD
,mysub.SHIP_BLOK_RSN_DESC AS SHIP_BLOK_RSN_DESC
,mysub.FCTRY_CAL_KEY AS FCTRY_CAL_KEY
,mysub.RTE_DESC AS RTE_DESC
,mysub.TRSPN_LEAD_TIME_IN_CAL_DAYS AS TRSPN_LEAD_TIME_IN_CAL_DAYS
,mysub.TRST_DUR_IN_CAL_DAYS AS TRST_DUR_IN_CAL_DAYS
,mysub._l0_upt_
from mysub mysub))where RNK =1   
 
"""
    )

    return out0