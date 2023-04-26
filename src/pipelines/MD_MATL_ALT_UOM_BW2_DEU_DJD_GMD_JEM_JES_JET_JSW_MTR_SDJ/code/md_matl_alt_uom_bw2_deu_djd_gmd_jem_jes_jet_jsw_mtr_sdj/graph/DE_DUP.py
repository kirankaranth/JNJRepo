from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def DE_DUP(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "/*\r\nThis gem is to remove any duplicates from the pipeline. \r\n\r\nSome duplicates arose (as expected) because of the method of doing 2 joins and the union; in examples where the source system \r\nhad the conversion ~ \"both ways\" e.g. EA --> CA = 12 AND CA --> EA = 0.0833.\r\n\r\nIn this situation I wanted to keep the 12 EA in a CA rather than an EA is 0.0833 of case. \r\nI managed this via the JOIN_ORIGIN being a number: 1 where the BASE_UOM = UMUM meaning the UMRUM (related) is the ALT. This\r\nensures that when both calculations exist, we only take the one that says that the ALT_UOM is x number of BASE_UOMs \r\nand not a BASE_UOM is 0.8 number of ALTs\r\n\r\nThere are examples about 30 out of 1 million (0.003%) where there is ONLY the ALT = 0.0833 of the BASE but that is the only data\r\nthere is so we have to go with it.\r\n\r\nThis row_num also takes care of the duplicates on JSW although it does mean that the values brought in are for a ~ random plant \r\n\r\n */\r\n\r\nSELECT \r\n    *\r\nFROM \r\n    (SELECT\r\n        in0.*,\r\n        row_number() OVER(PARTITION BY ITEM, ALT_UOM ORDER BY ITEM, ALT_UOM, JOIN_ORIGIN) row_num\r\n    FROM\r\n        in0) a\r\nWHERE row_num = 1\r\n"
    )

    return df1
