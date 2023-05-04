from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.config.ConfigStore import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.udfs.UDFs import *

def NEW_FIELDS_01(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("UOM_CD", trim(col("DRKY")))\
        .withColumn("EXTRNL_UOM_IN", lit(None))\
        .withColumn("EXTRNL_UOM_ID", lit(None))\
        .withColumn("DEC_PLACE_FOR_RD", lit(None))\
        .withColumn("COMML_MSR_UNIT_ID", lit(None))\
        .withColumn("VAL_BAS_CMMT_IN", lit(None))\
        .withColumn("UNIT_IN_1", lit(None))\
        .withColumn("UNIT_IN_2", lit(None))\
        .withColumn("DIM_KEY", trim(col("DRSPHD")))\
        .withColumn("NUMRTR_FOR_CONV_UNIT", lit(None))\
        .withColumn("DENOM_FOR_CONV_UNIT", lit(None))\
        .withColumn("BASE_TEN_EXP_FOR_CONV", lit(None))\
        .withColumn("ADDTV_CNST_FOR_CONV", lit(None))\
        .withColumn("EXP_OF_TEN_FOR_FLT_PT_FMT", lit(None))\
        .withColumn("NUM_OF_DEC_PLACE_FOR_NUM_DSPLY", lit(None))\
        .withColumn("ISO_CD_FOR_UOM", lit(None))\
        .withColumn("SLCT_FLD_FOR_CONV_FROM_ISO_CD_TO_INT_CD", lit(None))\
        .withColumn("TEMP", lit(None))\
        .withColumn("TEMP_UNIT", lit(None))\
        .withColumn("UOM_FMLY", lit(None))\
        .withColumn("PRESR_VAL", lit(None))\
        .withColumn("UNIT_OF_PRESR", lit(None))\
        .withColumn("EXTRNL_UOM_COMML_FMT", lit(None))\
        .withColumn("EXTRNL_UOM_TECH_FMT", lit(None))\
        .withColumn("UOM_SHRT_TEXT", trim(col("DRDL02")))\
        .withColumn("UOM_LONG_TEXT", trim(col("DRDL01")))\
        .withColumn("DIM_TEXT", lit(None))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'UOM_CD', UOM_CD)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'UOM_CD', UOM_CD)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
