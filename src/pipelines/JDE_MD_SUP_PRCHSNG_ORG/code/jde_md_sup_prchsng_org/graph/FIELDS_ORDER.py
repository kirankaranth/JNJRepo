from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_prchsng_org.config.ConfigStore import *
from jde_md_sup_prchsng_org.udfs.UDFs import *

def FIELDS_ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SUP_NUM"), 
        col("PRCHSNG_ORG_NUM"), 
        col("CRT_ON_DTTM"), 
        col("PRCH_BLK_IND"), 
        col("DEL_IND"), 
        col("CRNCY_CD"), 
        col("PMT_TERM_CD"), 
        col("INCOTERM1_CD"), 
        col("INCOTERM2_CD"), 
        col("PRC_PCDR_CD"), 
        col("PRC_CNTL_CD"), 
        col("EVAL_RCPT_SETLM_CD"), 
        col("RTRN_VEND_IND"), 
        col("CNFRM_CD"), 
        col("NM_OF_PRSN_RESP_CREAT_OBJ"), 
        col("GR_BAS_INVC_VERIF"), 
        col("AUTO_GNR_OF_PO_ALLW"), 
        col("AUTO_EVAL_RCPT_SETLM"), 
        col("OWN_EXPLN_OF_TERM_OF_PMT"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_deleted_")
    )
