from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_itm_node_deu_jem.config.ConfigStore import *
from jde_md_bom_itm_node_deu_jem.udfs.UDFs import *

def MD_BOM_ITM_NODE(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_BOM_ITM_NODE")
