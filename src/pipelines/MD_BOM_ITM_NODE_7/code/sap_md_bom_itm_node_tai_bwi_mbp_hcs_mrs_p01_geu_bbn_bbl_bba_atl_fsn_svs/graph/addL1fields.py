from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_itm_node_tai_bwi_mbp_hcs_mrs_p01_geu_bbn_bbl_bba_atl_fsn_svs.config.ConfigStore import *
from sap_md_bom_itm_node_tai_bwi_mbp_hcs_mrs_p01_geu_bbn_bbl_bba_atl_fsn_svs.udfs.UDFs import *

def addL1fields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'BOM_CAT_CD', BOM_CAT_CD, 'BOM_NUM', BOM_NUM, 'ALT_BOM_NUM', ALT_BOM_NUM, 'BOM_ITM_NODE_NUM', BOM_ITM_NODE_NUM, 'BOM_ITM_NODE_CNTR_NBR', BOM_ITM_NODE_CNTR_NBR)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'BOM_CAT_CD', BOM_CAT_CD, 'BOM_NUM', BOM_NUM, 'ALT_BOM_NUM', ALT_BOM_NUM, 'BOM_ITM_NODE_NUM', BOM_ITM_NODE_NUM, 'BOM_ITM_NODE_CNTR_NBR', BOM_ITM_NODE_CNTR_NBR)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
