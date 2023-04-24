from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_doc_ptnr_func.config.ConfigStore import *
from sap_01_md_sls_doc_ptnr_func.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.VBELN") == col("in1.VBELN")), "left_outer")\
        .select(col("in0.POSNR").alias("POSNR"), col("in0.VBELN").alias("VBELN"), col("in0.PARVW").alias("PARVW"), col("in1.BUKRS_VF").alias("BUKRS_VF"), col("in1.AUART").alias("AUART"), col("in0.PARVW_FF").alias("PARVW_FF"), col("in0.LIFNR").alias("LIFNR"), col("in0.ADRNR").alias("ADRNR"), col("in0.KUNNR").alias("KUNNR"), col("in0.LAND1").alias("LAND1"), col("in0.PERNR").alias("PERNR"), col("in0.PARNR").alias("PARNR"), col("in0.ABLAD").alias("ABLAD"), col("in0.ADRDA").alias("ADRDA"), col("in0.XCPDK").alias("XCPDK"), col("in0.HITYP").alias("HITYP"), col("in0.PRFRE").alias("PRFRE"), col("in0.BOKRE").alias("BOKRE"), col("in0.HISTUNR").alias("HISTUNR"), col("in0.KNREF").alias("KNREF"), col("in0.LZONE").alias("LZONE"), col("in0.HZUOR").alias("HZUOR"), col("in0.STCEG").alias("STCEG"), col("in0.ADRNP").alias("ADRNP"), col("in0.KALE").alias("KALE"), col("in0._upt_").alias("_upt_"))
