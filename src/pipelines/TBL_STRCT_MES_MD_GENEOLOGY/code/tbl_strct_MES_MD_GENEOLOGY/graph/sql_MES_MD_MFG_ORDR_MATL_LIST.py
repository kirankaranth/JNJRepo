from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_GENEOLOGY.config.ConfigStore import *
from tbl_strct_MES_MD_GENEOLOGY.udfs.UDFs import *

def sql_MES_MD_MFG_ORDR_MATL_LIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as MFG_ORDR_MATL_LIST_ITM_ID,\ncast('' as STRING) as MFG_ORDR_ID,\ncast('' as STRING) as PROD_ID,\ncast('' as BOOLEAN) as ALLW_OVR_CNSMPTN_IND,\ncast('' as BOOLEAN) as ALLW_SUBST_IND,\ncast('' as BOOLEAN) as ALLW_UND_CNSMPTN_IND,\ncast('' as STRING) as BTCH_ID,\ncast('' as BOOLEAN) as BULK_ITM_IND,\ncast('' as INT) as CHG_CNT,\ncast('' as BOOLEAN) as CO_PROD_IND,\ncast('' as STRING) as DFLT_LOT_NUM,\ncast('' as STRING) as DFLT_STK_PT_TXT,\ncast('' as TIMESTAMP) as EFF_FROM_DTTM,\ncast('' as TIMESTAMP) as EFF_FROM_GMT_DTTM,\ncast('' as TIMESTAMP) as EFF_THRU_DTTM,\ncast('' as TIMESTAMP) as EFF_THRU_GMT_DTTM,\ncast('' as BOOLEAN) as ERP_IS_PHANTOM_IND,\ncast('' as STRING) as ERP_LINE_ITEM_ID,\ncast('' as BOOLEAN) as ERP_RPTG_IND,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as BOOLEAN) as IS_ENABL_IND,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as BOOLEAN) as IS_PHANTOM_IND,\ncast('' as STRING) as ISS_CNTL_CD,\ncast('' as STRING) as ITM_CAT_CD,\ncast('' as STRING) as MFG_ORDR_MATL_LIST_ITM_NM,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as DECIMAL(18,4)) as ORIG_REQ_QTY,\ncast('' as STRING) as PHANTOM_BILL_ID,\ncast('' as STRING) as PROD_BASE_ID,\ncast('' as STRING) as PROD_DESC,\ncast('' as STRING) as REF_DSGNR_CD,\ncast('' as DECIMAL(18,4)) as REQ_QTY,\ncast('' as DECIMAL(18,4)) as REQ_2_QTY,\ncast('' as STRING) as RESV_ID,\ncast('' as STRING) as RESV_ITM_NUM,\ncast('' as STRING) as RTE_STEP_ID,\ncast('' as DECIMAL(9,4)) as SCRAP_PCT,\ncast('' as DECIMAL(18,4)) as SETUP_QTY,\ncast('' as DECIMAL(18,4)) as SETUP_2_QTY,\ncast('' as BOOLEAN) as SUB_FCTRY_IND,\ncast('' as STRING) as UOM_ID,\ncast('' as STRING) as UOM_2_ID,\ncast('' as BOOLEAN) as VEND_CNTL_IND,\ncast('' as BOOLEAN) as WHSE_IND,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
