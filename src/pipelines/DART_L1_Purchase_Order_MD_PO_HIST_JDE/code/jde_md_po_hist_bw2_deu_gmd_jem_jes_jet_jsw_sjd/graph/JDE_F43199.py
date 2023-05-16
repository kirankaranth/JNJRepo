from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_po_hist_bw2_deu_gmd_jem_jes_jet_jsw_sjd.config.ConfigStore import *
from jde_md_po_hist_bw2_deu_gmd_jem_jes_jet_jsw_sjd.udfs.UDFs import *

def JDE_F43199(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable}")
