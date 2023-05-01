from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.config.ConfigStore import *
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.udfs.UDFs import *

def NEW_FIELDS_01_T006(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("UOM_CD", col("MSEHI"))\
        .withColumn("EXTRNL_UOM_IN", trim(col("KZEX3")))\
        .withColumn("EXTRNL_UOM_ID", trim(col("KZEX6")))\
        .withColumn("DEC_PLACE_FOR_RD", trim(col("ANDEC")))\
        .withColumn("COMML_MSR_UNIT_ID", trim(col("KZKEH")))\
        .withColumn("VAL_BAS_CMMT_IN", trim(col("KZWOB")))\
        .withColumn("UNIT_IN_1", trim(col("KZ1EH")))\
        .withColumn("UNIT_IN_2", trim(col("KZ2EH")))\
        .withColumn("DIM_KEY", trim(col("DIMID")))\
        .withColumn("NUMRTR_FOR_CONV_UNIT", trim(col("ZAEHL")))\
        .withColumn("DENOM_FOR_CONV_UNIT", trim(col("NENNR")))\
        .withColumn("BASE_TEN_EXP_FOR_CONV", trim(col("EXP10")))\
        .withColumn("ADDTV_CNST_FOR_CONV", trim(col("ADDKO")))\
        .withColumn("EXP_OF_TEN_FOR_FLT_PT_FMT", trim(col("EXPON")))\
        .withColumn("NUM_OF_DEC_PLACE_FOR_NUM_DSPLY", trim(col("DECAN")))\
        .withColumn("ISO_CD_FOR_UOM", trim(col("ISOCODE")))\
        .withColumn("SLCT_FLD_FOR_CONV_FROM_ISO_CD_TO_INT_CD", trim(col("PRIMARY")))\
        .withColumn("TEMP", trim(col("TEMP_VALUE")))\
        .withColumn("TEMP_UNIT", trim(col("TEMP_UNIT")))\
        .withColumn("UOM_FMLY", trim(col("FAMUNIT")))\
        .withColumn("PRESR_VAL", trim(col("PRESS_VAL")))\
        .withColumn("UNIT_OF_PRESR", trim(col("PRESS_UNIT")))\
        .withColumn("EXTRNL_UOM_COMML_FMT", lookup("LU_T006A_MSEH3", col("MSEHI")).getField("MSEH3"))\
        .withColumn("EXTRNL_UOM_TECH_FMT", lookup("LU_T006A_MSEH6", col("MSEHI")).getField("MSEH6"))\
        .withColumn("UOM_SHRT_TEXT", lookup("LU_T006A_MSEHT", col("MSEHI")).getField("MSEHT"))\
        .withColumn("UOM_LONG_TEXT", lookup("LU_T006A_MSEHL", col("MSEHI")).getField("MSEHL"))\
        .withColumn("DIM_TEXT", lit(None))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'UOM_CD', UOM_CD)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'UOM_CD', UOM_CD)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
