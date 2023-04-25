from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.config.ConfigStore import *
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4211 = JDE_F4211(spark)
    df_JDE_F4211 = collectMetrics(
        spark, 
        df_JDE_F4211, 
        "graph", 
        "j3Yhz9Z01SOJCUhY9Zwmj$$ZPo5Vl17nASs0_yvonEGP", 
        "Vt1OAl6u2-EdrovshecSa$$q864S6ruskGa5EFTKe8lg"
    )
    df_JDE_F4211_FILTER = JDE_F4211_FILTER(spark, df_JDE_F4211)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JDE_F4211_FILTER)
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_JDE_F43121 = JDE_F43121(spark)
    df_JDE_F43121 = collectMetrics(
        spark, 
        df_JDE_F43121, 
        "graph", 
        "b6ZXYTjcrMjMRimRkxbaR$$uC7PkL8GZvFyEnn-VhLsa", 
        "41meaBo75DSc-100lRJr2$$HkFGgAN84fPBqZaHsJLez"
    )
    df_JDE_F43121_FILTER = JDE_F43121_FILTER(spark, df_JDE_F43121)
    df_NEW_FIELDS_RENAME_FORMAT_2 = NEW_FIELDS_RENAME_FORMAT_2(spark, df_JDE_F43121_FILTER)
    df_SET_FIELD_ORDER_FORMAT_1 = SET_FIELD_ORDER_FORMAT_1(spark, df_NEW_FIELDS_RENAME_FORMAT_2)
    df_Deduplicate_1 = Deduplicate_1(spark, df_SET_FIELD_ORDER_FORMAT_1)
    df_UNION = UNION(spark, df_SET_FIELD_ORDER_FORMAT, df_Deduplicate_1)
    df_UNION = collectMetrics(
        spark, 
        df_UNION, 
        "graph", 
        "F0LqdVBQUi6qGG1QK6-Pq$$oj4yuFyENpKD9UYnem-ya", 
        "PfsuMK7fPfUgOBsN6DkAG$$_owsJjLoKKyTZ-aJ_T2a5"
    )
    df_DUPLICATE = DUPLICATE(spark, df_UNION)
    MD_SHIP(spark, df_UNION)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "3nI35cIGF9OVRoaMLbvq2$$f3yiCQVZ37ZXgQtS517Zp", 
        "Yw4HfAn29dxa1wpuNa3JQ$$O69W8PE8t_0n1dC3FeKAB"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JES_01_MD_SHIP_JET_JSW_DEU_JEM_SJD_DJD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JES_01_MD_SHIP_JET_JSW_DEU_JEM_SJD_DJD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
