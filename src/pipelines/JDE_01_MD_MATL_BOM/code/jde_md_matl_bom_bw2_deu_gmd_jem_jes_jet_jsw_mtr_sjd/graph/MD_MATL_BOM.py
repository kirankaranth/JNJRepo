from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.config.ConfigStore import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.udfs.UDFs import *

def MD_MATL_BOM(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MATL_BOM")
