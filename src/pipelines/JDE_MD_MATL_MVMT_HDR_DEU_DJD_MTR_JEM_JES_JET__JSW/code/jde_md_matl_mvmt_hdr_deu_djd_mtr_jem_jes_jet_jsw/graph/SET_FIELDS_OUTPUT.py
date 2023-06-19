from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.udfs.UDFs import *

def SET_FIELDS_OUTPUT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_MVMT_NUM"), 
        col("MATL_MVMT_YR"), 
        col("EV_TYPE_CD"), 
        col("MATL_MVMT_DTTM"), 
        col("PSTNG_DTTM"), 
        col("MATL_MVMT_HDR_TXT"), 
        col("DOC_TYPE"), 
        col("DOC_TYPE_DESC"), 
        col("LAST_CHG_DTTM"), 
        col("SPLT_GUID_PART1"), 
        col("SPLT_GUID_PART2"), 
        col("SPLT_GUID_PART3"), 
        col("SPLT_GUID_PART4"), 
        col("SPLT_GUID_PART5"), 
        col("SPLT_GUID_PART6"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
