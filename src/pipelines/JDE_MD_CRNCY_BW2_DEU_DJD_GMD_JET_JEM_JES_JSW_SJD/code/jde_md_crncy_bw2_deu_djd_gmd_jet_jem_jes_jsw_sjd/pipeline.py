from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.config.ConfigStore import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F0013 = DS_JDE_01_F0013(spark)
    df_DS_JDE_01_F0013 = collectMetrics(
        spark, 
        df_DS_JDE_01_F0013, 
        "graph", 
        "U-S3TYvPIXQfWSlm0hR7u$$ShLBA9_aG4sYDyoC_wsRu", 
        "7SZ3Djl1jTPZbl0ahZrto$$d-0Z1X9_jQPzgnOo5cuI1"
    )
    df_DELETED_FILTER_F0013 = DELETED_FILTER_F0013(spark, df_DS_JDE_01_F0013)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_DELETED_FILTER_F0013)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "Iy_GE-n0m198FG3R7mpO0$$_QalUL53DKoc8TeVgAvps", 
        "bw5PW_ucTPaziGAksIJXc$$Tb6hpyCzK75Cc2xb4FZSk"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "hsSTT0eAkbyhLrxN-efg4$$ADOZG3UIyExnhIMKx8Ek1", 
        "iHhXvJas_TDQvjCTKZvL5$$8MqEssofokQ3kTd8WOFSC"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    MD_CRNCY(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_CRNCY_BW2_DEU_DJD_GMD_JET_JEM_JES_JSW_SJD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_CRNCY_BW2_DEU_DJD_GMD_JET_JEM_JES_JSW_SJD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
