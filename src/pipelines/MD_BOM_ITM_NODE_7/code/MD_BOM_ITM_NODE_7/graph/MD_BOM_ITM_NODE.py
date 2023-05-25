from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_BOM_ITM_NODE_7.config.ConfigStore import *
from MD_BOM_ITM_NODE_7.udfs.UDFs import *

def MD_BOM_ITM_NODE(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_BOM_ITM_NODE")
