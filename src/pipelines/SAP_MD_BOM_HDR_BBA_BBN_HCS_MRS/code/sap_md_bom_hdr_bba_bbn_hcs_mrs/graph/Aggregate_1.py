from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_hdr_bba_bbn_hcs_mrs.config.ConfigStore import *
from sap_md_bom_hdr_bba_bbn_hcs_mrs.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SRC_SYS_CD"), col("BOM_CAT_CD"), col("BOM_NUM"), col("ALT_BOM_NUM"), col("BOM_CNTR_NBR"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
