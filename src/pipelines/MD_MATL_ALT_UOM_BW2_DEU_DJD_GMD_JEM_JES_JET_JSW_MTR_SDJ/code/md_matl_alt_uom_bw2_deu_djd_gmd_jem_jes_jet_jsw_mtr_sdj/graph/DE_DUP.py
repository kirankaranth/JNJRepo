from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def DE_DUP(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "SELECT \r\n    *\r\nFROM \r\n    (SELECT\r\n        in0.*,\r\n        row_number() OVER(PARTITION BY ITEM, ALT_UOM ORDER BY ITEM, ALT_UOM, JOIN_ORIGIN) row_num\r\n    FROM\r\n        in0) a\r\nWHERE row_num = 1\r\n"
    )

    return df1
