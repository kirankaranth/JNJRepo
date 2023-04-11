from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_line_.config.ConfigStore import *
from jde_md_sls_ordr_line_.udfs.UDFs import *

def SET_FIELDS_REFORMAT_ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("COMPANY_CD"), 
        col("SLS_ORDR_DOC_ID"), 
        col("SLS_ORDR_LINE_NBR"), 
        col("SLS_ORDR_TYPE_CD"), 
        col("CNFRM_QTY"), 
        col("SLS_UNIT_ORDR_QTY"), 
        col("SLOC_CD"), 
        col("MATL_NUM"), 
        col("NET_VAL_AMT"), 
        col("RTE_ID"), 
        col("SLS_UOM_CD"), 
        col("SLS_ORDR_CRNCY_CD"), 
        col("PLNT_CD"), 
        col("INTNL_COM_CD"), 
        col("CNFRM_STS_CD"), 
        col("PRCSG_TOT_STS_CD"), 
        col("PICK_STS_CD"), 
        col("DELV_STS_CD"), 
        col("CR_DTTM"), 
        col("BASE_UOM_CD"), 
        col("AVAIL_TO_PROM_DTTM"), 
        col("GRS_WT_MEAS"), 
        col("WT_UOM_CD"), 
        col("NET_WT_MEAS"), 
        col("VOL_UOM_CD"), 
        col("VOL_MEAS"), 
        col("COST_IN_DOC_CRNCY"), 
        col("BTCH_NUM"), 
        col("ORIG_RQST_DELV_DTTM"), 
        col("CHG_DTTM"), 
        col("DR_CR_IN"), 
        col("CRT_BY_NM"), 
        col("MATL_GRP_1"), 
        col("MATL_GRP_2_MVGR2"), 
        col("MATL_GRP_3"), 
        col("MATL_GRP_4"), 
        col("MATL_GRP_5"), 
        col("OVRD_PRC_RES_CD"), 
        col("TAX_CLSN_FOR_MATL_1"), 
        col("TAX_CLSN_FOR_MATL_2"), 
        col("ACCT_ASGNMT_GRP"), 
        col("STATS_EXCH_RT"), 
        col("BILL_DTTM"), 
        col("SHIP_TO_PRTY"), 
        col("BLLT_PRTY"), 
        col("CNCL_DTTM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_md5_"), 
        col("_l1_upt_")
    )
