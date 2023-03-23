from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_BILLING.config.ConfigStore import *
from tbl_strct_MD_BILLING.udfs.UDFs import *

def sql_MD_BILL_DOC_HDR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as BILL_DOC,\ncast('' as string) as SLORG_CD,\ncast('' as string) as SLORG_DESC,\ncast('' as string) as DSTR_CHNL_CD,\ncast('' as string) as DSTR_CHNL_DESC,\ncast('' as string) as SLS_DIV_CD,\ncast('' as string) as SLS_DIV_DESC,\ncast('' as string) as BILL_TYPE_CD,\ncast('' as string) as BILL_TYPE_DESC,\ncast('' as string) as BILL_CAT,\ncast('' as string) as DOC_CAT,\ncast('' as string) as PYR,\ncast('' as string) as SOLD_TO,\ncast('' as string) as SHIP_TO,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as BILL_DTTM,\ncast('' as timestamp) as BILL_INVC_DTTM,\ncast('' as string) as CRNCY_CD,\ncast('' as string) as CRT_BY,\ncast('' as string) as PRC_PCDR_CD,\ncast('' as string) as DOC_COND_OWN_COND,\ncast('' as string) as SHIPPING_COND_CD,\ncast('' as string) as FISC_YR,\ncast('' as int) as PSTNG_PER,\ncast('' as string) as CUST_GRP_CD,\ncast('' as string) as INTNL_COM_CD,\ncast('' as string) as DEL_DPRT_PT_CD,\ncast('' as string) as PSTNG_STS,\ncast('' as decimal (18,4)) as EXCH_RT_FIN_PSTNG,\ncast('' as string) as ADDL_VAL_DAYS,\ncast('' as timestamp) as FX_VAL_DTTM,\ncast('' as string) as PMT_TERM_CD,\ncast('' as string) as ACCT_ASGNMT_GRP,\ncast('' as string) as CTRY_CD,\ncast('' as string) as REGION_CD,\ncast('' as string) as CO_CD,\ncast('' as string) as TAX_CLSN_1,\ncast('' as decimal (18,4)) as NET_VAL_AMT,\ncast('' as string) as COMB_CRITA,\ncast('' as string) as STATS_CRNCY,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as string) as INVC_LIST_TYPE,\ncast('' as string) as CNTL_AREA_CD,\ncast('' as string) as CR_ACCT,\ncast('' as string) as CRNCY_CR_CNTL_AREA,\ncast('' as decimal (18,4)) as CR_DX_RT,\ncast('' as string) as HIER_TYPE_PRC,\ncast('' as string) as CUST_PO_NUM,\ncast('' as string) as TRAD_PTNR_CO_CD,\ncast('' as string) as TAX_DPRT_CTRY,\ncast('' as string) as ORIG_VAT_NUM,\ncast('' as string) as CTRY_VAT_NUM,\ncast('' as string) as REF_DOC_NUM,\ncast('' as string) as ASGNMT_NUM,\ncast('' as decimal (18,4)) as TAX_AMT,\ncast('' as string) as LOGL_SYS,\ncast('' as timestamp) as TRNL_DTTM,\ncast('' as string) as PMT_REF,\ncast('' as int) as NUM_OF_PG,\ncast('' as string) as PSTNG_BILL_STS_CD,\ncast('' as string) as INVC_LIST_STS_CD,\ncast('' as string) as CUST_PRC_GRP,\ncast('' as string) as SLS_DSTRC,\ncast('' as string) as PRC_LIST_TYPE,\ncast('' as string) as TAX_CLSN_2,\ncast('' as string) as TAX_CLSN_3,\ncast('' as string) as TAX_CLSN_4,\ncast('' as string) as TAX_CLSN_5,\ncast('' as string) as TAX_CLSN_6,\ncast('' as string) as TAX_CLSN_7,\ncast('' as string) as TAX_CLSN_8,\ncast('' as string) as TAX_CLSN_9,\ncast('' as string) as INDSTR_CD_1,\ncast('' as string) as INDSTR_CD_2,\ncast('' as string) as PRCH_ORDR_TYPE,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1