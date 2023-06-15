from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.config.ConfigStore import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.udfs.UDFs import *

def SAP_VBAP(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.vbap")
