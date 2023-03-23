from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_BILL_OF_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_BILL_OF_MATERIAL.udfs.UDFs import *

def sql_MD_BOM_ITM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as BOM_CAT_CD,\ncast('' as string) as BOM_NUM,\ncast('' as string) as BOM_ITM_NODE_NUM,\ncast('' as string) as BOM_ITM_CNTR_NBR,\ncast('' as timestamp) as BOM_ITM_VLD_FROM_DTTM,\ncast('' as string) as CHG_NUM,\ncast('' as string) as DEL_IND,\ncast('' as string) as PREV_NODE_NUM,\ncast('' as string) as PREV_ITM_CNTR_NBR,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as string) as CMPNT_NUM,\ncast('' as string) as BOM_ITM_CAT_CD,\ncast('' as string) as BOM_ITM_NUM,\ncast('' as string) as CMPNT_UOM_CD,\ncast('' as decimal(18,4)) as CMPNT_QTY,\ncast('' as string) as FX_QTY_IND,\ncast('' as decimal(18,4)) as CMPNT_SCRAP_QTY_PCT,\ncast('' as decimal(18,4)) as OPR_SCRAP_QTY_PCT,\ncast('' as string) as BOM_ITM_BULK_IND,\ncast('' as decimal(18,4)) as LEAD_TIME_OFFST_DAYS_QTY,\ncast('' as string) as DSTN_KEY_CD,\ncast('' as string) as ALT_ITM_IND,\ncast('' as decimal(18,4)) as ALT_ITM_QTY_PCT,\ncast('' as string) as BOM_ITM_TXT,\ncast('' as decimal(18,4)) as GR_DAYS_LEAD_QTY,\ncast('' as string) as CO_PROD_IND,\ncast('' as string) as ALT_ITM_STRTGY_CD,\ncast('' as string) as ALT_ITM_RNKNG_NBR,\ncast('' as string) as ALT_ITM_GRP_CD,\ncast('' as timestamp) as BOM_ITM_VLD_TO_DTTM,\ncast('' as string) as ISU_PLNT,\ncast('' as string) as TECH_STS_FROM,\ncast('' as string) as USER_WHO_CRT_REC,\ncast('' as string) as NM_OF_PRSN_WHO_CHG_OBJ,\ncast('' as string) as SRT_STRNG,\ncast('' as string) as NET_SCRAP_IN,\ncast('' as string) as MATL_PRVSN_IN,\ncast('' as string) as SPARE_PART_IN,\ncast('' as string) as ITM_RLVNT_TO_SLS_IN,\ncast('' as string) as ITM_RLVNT_TO_PRDTN_IN,\ncast('' as string) as ITM_RLVNT_TO_PLNT_MAINT_IN,\ncast('' as string) as FOR_RLVNC_TO_COST_IN,\ncast('' as string) as ITM_RLVNT_TO_ENGR_IN,\ncast('' as string) as HIGH_LVL_CNFG_IN,\ncast('' as string) as PM_ASBL_IN,\ncast('' as string) as BOM_IS_RCURSV_IN,\ncast('' as string) as RCURSV_ALLW_IN,\ncast('' as string) as CAD_IN,\ncast('' as string) as FLLP_MATL_BOM_ITM,\ncast('' as string) as PRCHSNG_GRP,\ncast('' as decimal (18,4)) as DELV_TIME_IN_DAYS,\ncast('' as string) as ACCT_NUM_OF_SUP,\ncast('' as decimal (18,4)) as PRICE,\ncast('' as decimal (18,4)) as PRC_UNIT,\ncast('' as string) as CRNCY_KEY,\ncast('' as string) as COST_ELMNT,\ncast('' as decimal (18,4)) as REQ_NUM_OF_VAR_SIZE_ITM,\ncast('' as decimal (18,4)) as SIZE_1,\ncast('' as decimal (18,4)) as SIZE_2,\ncast('' as decimal (18,4)) as SIZE_3,\ncast('' as string) as UNIT_OF_MEAS_FOR_SZ_1_TO_3,\ncast('' as decimal (18,4)) as VAR_SZ_ITM_QTY_PER_PIECE,\ncast('' as string) as FRML_KEY_FOR_VAR_SZ_ITM,\ncast('' as string) as SUB_ITM_EXIST_IN,\ncast('' as string) as ITM_IN_MORE_THN_ONE_ALT,\ncast('' as string) as LONG_TEXT_LANG,\ncast('' as string) as OBJ_TYPE,\ncast('' as string) as MATL_GRP,\ncast('' as string) as DOC_TYPE,\ncast('' as string) as DOC_NUM,\ncast('' as string) as DOC_VERS,\ncast('' as string) as DOC_PART,\ncast('' as decimal (18,4)) as AVG_MATL_PURITY_IN_PERCNT,\ncast('' as string) as CLS_NUM,\ncast('' as string) as CLS_TYPE,\ncast('' as string) as RSLTNG_ITM_CAT,\ncast('' as string) as SLCT_IN_FOR_CNFG_BOM,\ncast('' as string) as INST_IN,\ncast('' as string) as NOT_DSPL_IN_CNFG_EDITR_IN,\ncast('' as string) as NOT_DSPL_IN_SNGL_LVL_CNFG,\ncast('' as string) as NOT_DSPL_IN_AUTO_CNFG_IN,\ncast('' as string) as PRCHSNG_ORG,\ncast('' as string) as REQ_CMPNT,\ncast('' as string) as MLT_SLCT_ALLW,\ncast('' as string) as ALT_DSPLY_FMT,\ncast('' as string) as ORG_AREA,\ncast('' as string) as NUM_OF_OBJ_WTH_ASGN_DPND,\ncast('' as string) as ISS_LOC_FOR_PRDTN_ORDR,\ncast('' as string) as INTRA_MATL,\ncast('' as string) as RSTRC_EXIST_IN,\ncast('' as string) as INHRT_NODE_NUM_OF_BOM_ITM,\ncast('' as timestamp) as LAST_DT_SHFT_DTTM,\ncast('' as string) as NM_OF_THE_USER,\ncast('' as string) as EXPLS_TYPE,\ncast('' as string) as FLLP_ITM_IN,\ncast('' as string) as FLLP_GRP,\ncast('' as string) as DSCNT_GRP,\ncast('' as string) as MAN_CHG_TO_SLS_ORDR_BOM_IN,\ncast('' as string) as BOM_ITM_CHG_DPND_IN,\ncast('' as string) as BOM_CAT_OF_ORIG_SLS_ORDR,\ncast('' as string) as BOM_FOR_ORIG_SLS_ORDR,\ncast('' as string) as NODE_NUM_OF_ORIG_SLS_ORDR,\ncast('' as string) as CNTR_FOR_ORIG_SLS_ORDR,\ncast('' as string) as CLSN_NUM,\ncast('' as string) as CLSN_AS_SLCT_COND_IN,\ncast('' as string) as DT_SHFT_HIER_IN,\ncast('' as string) as PRDTN_SUPL_AREA,\ncast('' as decimal (18,4)) as LDTM_OFFST_FOR_OPR,\ncast('' as string) as UNIT_FOR_LDTM_OFFST_FOR_OPR,\ncast('' as string) as ITM_GRP,\ncast('' as string) as HIST_CNTR,\ncast('' as string) as CMPNT_VRNT,\ncast('' as string) as ALE_IN,\ncast('' as string) as EXTRNL_ID_OF_AN_ITM,\ncast('' as string) as TEMP_NOT_USED,\ncast('' as string) as SPL_PRCMT_TYPE_FOR_BOM_ITM,\ncast('' as string) as REF_PT_FOR_BOM_TFR,\ncast('' as string) as GLOBL_ID_OF_AN_ITM_CHG_STS,\ncast('' as string) as SEG_MANTN_FOR_BOM_CMPNT,\ncast('' as string) as SEG_VAL,\ncast('' as timestamp) as VALID_TO_DTTM,\ncast('' as string) as CHG_NUM_TO,\ncast('' as string) as CHG_NUM_TO_1,\ncast('' as string) as INHRT_NODE_NUM_OF_BOM_ITM_1,\ncast('' as timestamp) as UTC_TIME_STMP_LONG_DTTM,\ncast('' as string) as NUM_OF_CU_INSTNCE,\ncast('' as string) as DEVT_VAL_MANTN_FOR_CMPNT,\ncast('' as string) as QTY_DSTN_PRFL,\ncast('' as string) as REF_TO_QTY_DSTN_PRFL,\ncast('' as string) as CRIT_CMPNT_IN,\ncast('' as decimal (18,4)) as CRIT_LVL_OF_A_CMPNT_IN_BOM,\ncast('' as string) as FUNC_ID,\ncast('' as string) as SER_NUM,\ncast('' as string) as BTCH_NUM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
