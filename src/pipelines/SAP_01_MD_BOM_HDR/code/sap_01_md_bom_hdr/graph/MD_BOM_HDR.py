from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bom_hdr.config.ConfigStore import *
from sap_01_md_bom_hdr.udfs.UDFs import *

def MD_BOM_HDR(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("error").saveAsTable(f"l1_md_prophecy.md_bom_hdr")
