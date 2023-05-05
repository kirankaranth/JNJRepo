from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.config.ConfigStore import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.udfs.UDFs import *

def SET_FIELD_ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("UOM_CD"), 
        col("EXTRNL_UOM_IN"), 
        col("EXTRNL_UOM_ID"), 
        col("DEC_PLACE_FOR_RD"), 
        col("COMML_MSR_UNIT_ID"), 
        col("VAL_BAS_CMMT_IN"), 
        col("UNIT_IN_1"), 
        col("UNIT_IN_2"), 
        col("DIM_KEY"), 
        col("NUMRTR_FOR_CONV_UNIT"), 
        col("DENOM_FOR_CONV_UNIT"), 
        col("BASE_TEN_EXP_FOR_CONV"), 
        col("ADDTV_CNST_FOR_CONV"), 
        col("EXP_OF_TEN_FOR_FLT_PT_FMT"), 
        col("NUM_OF_DEC_PLACE_FOR_NUM_DSPLY"), 
        col("ISO_CD_FOR_UOM"), 
        col("SLCT_FLD_FOR_CONV_FROM_ISO_CD_TO_INT_CD"), 
        col("TEMP"), 
        col("TEMP_UNIT"), 
        col("UOM_FMLY"), 
        col("PRESR_VAL"), 
        col("UNIT_OF_PRESR"), 
        col("EXTRNL_UOM_COMML_FMT"), 
        col("EXTRNL_UOM_TECH_FMT"), 
        col("UOM_SHRT_TEXT"), 
        col("UOM_LONG_TEXT"), 
        col("DIM_TEXT"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
