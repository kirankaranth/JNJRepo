from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.config.ConfigStore import *
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.udfs.UDFs import *

def MD_SHIP(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SHIP")
