from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.config.ConfigStore import *
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_F0101_adt = DS_JDE_F0101_adt(spark)
    df_DS_JDE_F0101_adt = collectMetrics(
        spark, 
        df_DS_JDE_F0101_adt, 
        "graph", 
        "pOhePJ2y3XrQ0CqBcZG5t$$egSZA1Bab_xa_YW6Eftl4", 
        "yBEqnYoWsymFpTjrJ88WK$$_HNA_LSxn7jnxwtsv0ksF"
    )
    df_DELETED_FILTER = DELETED_FILTER(spark, df_DS_JDE_F0101_adt)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_DELETED_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "wpbjSdUAWmgXs4SzjzboD$$B2vch_y5LIxy2bS9i3K-Y", 
        "LNFYNK8KND7h0SG0lRX3o$$qBqDZ1yqddUbG5qVlTPcO"
    )
    MD_CO_CD(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "rCAVUFUZmlBLfGWPswzxy$$Bp5zQspqNJdRwakbJ6eof", 
        "qewNllJxQ392ROeJVB-AV$$obUaTQpeVzzPb0o93EVjn"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_CO_CD_BW2_JET_DJD_JES")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_CO_CD_BW2_JET_DJD_JES")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
