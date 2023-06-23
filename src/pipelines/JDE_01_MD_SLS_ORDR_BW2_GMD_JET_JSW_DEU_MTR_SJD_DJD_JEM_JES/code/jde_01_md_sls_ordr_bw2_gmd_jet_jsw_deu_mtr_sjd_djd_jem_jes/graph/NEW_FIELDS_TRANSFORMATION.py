from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("COMPANY_CD", trim(col("SHKCOO")))\
        .withColumn("SLS_ORDR_DOC_ID", trim(col("SHDOCO")))\
        .withColumn("SLS_ORDR_TYPE_CD", trim(col("SHDCTO")))\
        .withColumn(
          "CRT_DTTM",
          when(
              (
                ((lower(trim(col("SHTRDJ"))) == lit("null")) | (trim(col("SHTRDJ")) == lit("")))
                | (trim(col("SHTRDJ")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("SHTRDJ")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("SHTRDJ")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 4)\
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
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
