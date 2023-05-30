from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.config.ConfigStore import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.udfs.UDFs import *

def DE_DUPLICATE(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "SELECT \r\n    *\r\nFROM\r\n    (SELECT \r\n        OBJNR AS JEST_OBJNR, \r\n        STAT, \r\n        row_number() OVER(PARTITION BY OBJNR ORDER BY OBJNR ASC, STAT DESC) rank_no \r\n    FROM \r\n        in0) a\r\nWHERE rank_no = 1"
    )

    return df1
