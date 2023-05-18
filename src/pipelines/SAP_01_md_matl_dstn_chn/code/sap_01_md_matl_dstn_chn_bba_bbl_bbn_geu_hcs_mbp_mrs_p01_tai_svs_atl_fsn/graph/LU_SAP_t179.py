from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn.config.ConfigStore import *
from sap_01_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn.udfs.UDFs import *

def LU_SAP_t179(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''PRODH''']
    valueColumns = ['''STUFE''']
    createLookup("LU_SAP_t179", in0, spark, keyColumns, valueColumns)
