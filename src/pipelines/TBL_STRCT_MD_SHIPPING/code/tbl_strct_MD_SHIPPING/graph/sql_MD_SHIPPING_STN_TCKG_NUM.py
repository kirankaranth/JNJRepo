from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_SHIPPING.config.ConfigStore import *
from tbl_strct_MD_SHIPPING.udfs.UDFs import *

def sql_MD_SHIPPING_STN_TCKG_NUM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as SLS_AND_DSTN_DOC_NUM,\ncast('' as String) as INTRNL_HNDG_UNIT_NUM,\ncast('' as String) as SHIPPING_PT_RECV_PT,\ncast('' as String) as SHIPPING_STN_TCKG_NUM,\ncast('' as String) as CHECKBOX_VOIDED,\ncast('' as decimal(18,4)) as GRS_WT,\ncast('' as String) as CRNCY_KEY,\ncast('' as String) as WT_UNIT,\ncast('' as decimal(18,4)) as STD_PRC_NET_CHRG,\ncast('' as decimal(18,4)) as GRS_WT_BLLD,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as String) as CRT_USER_NM,\ncast('' as timestamp) as CHNG_DTTM,\ncast('' as String) as CHNG_USER_NM,\ncast('' as String) as SHIPPING_STN,\ncast('' as String) as LGTH_WDTH_HGHT_PKG_SHIP,\ncast('' as String) as EXP_DELV_CO,\ncast('' as String) as SHIPPING_STN_TCKG_BARCD_LBL_DATA,\ncast('' as String) as SHIPPING_STN_VEND_CLS_PCDR_GRP,\ncast('' as timestamp) as SHIPPING_STN_CLSE_DTTM,\ncast('' as String) as SHIPPING_STN_CLSE_NUM,\ncast('' as String) as SHIPPING_STN_MTR_NUM,\ncast('' as timestamp) as SHIPPING_STN_SHIP_PARCEL_TCKG_DTTM,\ncast('' as String) as SHIPPING_STN_SHIPPING_ID,\ncast('' as String) as SHIPPING_STN_PKG_MSTR_SEQ_NUM,\ncast('' as timestamp) as SHIPDATE_DTTM,\ncast('' as String) as SHIPPING_ACCT_CHRG_PKG,\ncast('' as decimal(18,4)) as STD_PRC_COD_AMT,\ncast('' as String) as SRVC_LVL_PKG_SHIP_WTH,\ncast('' as decimal(18,4)) as STD_PRC_COD_VAL,\ncast('' as decimal(18,4)) as STD_PRC_NET_LIST_CHRG,\ncast('' as decimal(18,4)) as STD_PRC_BASE_CHRG,\ncast('' as decimal(18,4)) as STD_PRC_BASE_LIST_CHRG,\ncast('' as decimal(18,4)) as STD_PRC_TOV_SVC_CHRG,\ncast('' as String) as PKG_BNDLE_ID,\ncast('' as String) as SHIPPING_ZN,\ncast('' as decimal(18,4)) as STD_PRC_FUEL_SURC,\ncast('' as timestamp) as SYS_DELVY_DTTM,\ncast('' as String) as SHIPPING_STN_STD_TRST_DAYS,\ncast('' as String) as TCKG_STS,\ncast('' as timestamp) as SYS_DELVD_DTTM,\ncast('' as String) as TNT_ACCS_CD,\ncast('' as String) as GRP_CD_SHIP,\ncast('' as String) as CONSG_NOTE_NUM,\ncast('' as String) as TNT_MNFST_ACCS_CD,\ncast('' as String) as LOGL_DEST_SPEC_IN_FUNC,\ncast('' as String) as EXTRNL_HNDG_UNIT_ID,\ncast('' as String) as CHECKBOX_EWM_VENUM,\ncast('' as String) as WORLDEASE,\ncast('' as String) as CONS_ID,\ncast('' as String) as IOR_FAC_CD,\ncast('' as String) as UNIQ_INTRNL_ID_HNDG_UNIT,\ncast('' as String) as PKG_NOT_RELS_MNFST,\ncast('' as decimal(18,4)) as SAT_DELV_ACCS_OPT_CHRG,\ncast('' as decimal(18,4)) as LIFTGATE_DELV_ACCS_OPT_CHRG,\ncast('' as decimal(18,4)) as RSDNL_DELV_ACCS_OPT_CHRG,\ncast('' as decimal(18,4)) as INSID_DELV_ACCS_OPT_CHRG,\ncast('' as String) as INTRNL_HNDG_UNIT_NUM2,\ncast('' as String) as DGIS_SHIP,\ncast('' as String) as NODE_ID,\ncast('' as String) as CONS_NUM,\ncast('' as String) as HU_NUMRTR,\ncast('' as String) as HU_DENOM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
