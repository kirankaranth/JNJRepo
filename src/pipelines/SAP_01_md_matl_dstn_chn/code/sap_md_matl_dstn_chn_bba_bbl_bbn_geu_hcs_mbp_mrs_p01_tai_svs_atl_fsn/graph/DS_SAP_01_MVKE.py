from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn.config.ConfigStore import *
from sap_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn.udfs.UDFs import *

def DS_SAP_01_MVKE(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mvke")
