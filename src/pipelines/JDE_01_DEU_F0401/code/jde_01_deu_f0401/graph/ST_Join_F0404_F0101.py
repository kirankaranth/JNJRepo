from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def ST_Join_F0404_F0101(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit("check it later"))\
        .withColumnRenamed("A6AN8", "SUP_NUM")\
        .withColumn("PRCHSNG_ORG_NUM", lit(None))\
        .withColumn(
          "CRT_ON_DTTM",
          when(
              (
                ((lower(trim(col("a6upmj"))) == lit("null")) | (trim(col("a6upmj")) == lit("")))
                | (trim(col("a6upmj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("a6upmj")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("a6upmj")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 3)\
                      .cast(IntegerType())
                    - 1
                  )
                )\
                .cast(StringType()), 
              1, 
              10
            ), 
            "yyyy-MM-dd"
          ))
        )\
        .withColumn("PRCH_BLK_IND", lit(None))\
        .withColumn("DEL_IND", lit(None))\
        .withColumn("CRNCY_CD", trim(col("A6CRRP")))
