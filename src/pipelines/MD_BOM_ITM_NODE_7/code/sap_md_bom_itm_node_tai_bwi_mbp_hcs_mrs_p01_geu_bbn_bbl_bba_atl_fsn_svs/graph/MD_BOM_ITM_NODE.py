from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_itm_node_tai_bwi_mbp_hcs_mrs_p01_geu_bbn_bbl_bba_atl_fsn_svs.config.ConfigStore import *
from sap_md_bom_itm_node_tai_bwi_mbp_hcs_mrs_p01_geu_bbn_bbl_bba_atl_fsn_svs.udfs.UDFs import *

def MD_BOM_ITM_NODE(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_BOM_ITM_NODE")
