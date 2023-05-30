from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_sup_prchsng_org.config.ConfigStore import *
from jde_md_sup_prchsng_org.udfs.UDFs import *
from prophecy.utils import *
from jde_md_sup_prchsng_org.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F0401 = JDE_F0401(spark)
    df_JDE_F0401 = collectMetrics(
        spark, 
        df_JDE_F0401, 
        "graph", 
        "MwjLke673GX7Fy5Pwqn2Z$$nCI0PT-DhrkCGQevY_0-O", 
        "JnWKGkK-Ub6gNy3MTJ_Br$$7Kth7yn0F2eKKrysySi3o"
    )
    df_JDE_FILTER = JDE_FILTER(spark, df_JDE_F0401)
    df_REFORMAT_F0401 = REFORMAT_F0401(spark, df_JDE_FILTER)
    df_JDE_F0101 = JDE_F0101(spark)
    df_JDE_F0101 = collectMetrics(
        spark, 
        df_JDE_F0101, 
        "graph", 
        "wMhRYnuAgn9mylV_DBwHI$$kHVNO7H1jYLxf_zY6He7b", 
        "2cLaiFoBttKw6MyBqneqn$$eNwpQeHx9Byu9WH5qy1cm"
    )
    df_JDE_FILTER_1 = JDE_FILTER_1(spark, df_JDE_F0101)
    df_REFORMAT_F0101 = REFORMAT_F0101(spark, df_JDE_FILTER_1)
    df_JOIN = JOIN(spark, df_REFORMAT_F0401, df_REFORMAT_F0101)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_JOIN)
    df_PK_CHECK = PK_CHECK(spark, df_NEW_FIELDS)
    df_ORDER_FIELDS = ORDER_FIELDS(spark, df_NEW_FIELDS)
    df_ORDER_FIELDS = collectMetrics(
        spark, 
        df_ORDER_FIELDS, 
        "graph", 
        "iIpp_XYE8VLc9YFlVypbv$$QwvhBf1EgCjiWQV27x4Lk", 
        "4NNGlK5Kyat1X_k7lJ1hm$$BC7QSi0-u6HZssfa-i68k"
    )
    MD_SUP_PRCHSNG_ORG(spark, df_ORDER_FIELDS)
    df_PK_FILTER = PK_FILTER(spark, df_PK_CHECK)
    df_PK_FILTER = collectMetrics(
        spark, 
        df_PK_FILTER, 
        "graph", 
        "ue78PcoMUmSseoIn7-LWw$$uEUXOmg9plg3yvPRnosEU", 
        "65h-cpTB_6QvRoTk8mofT$$2DhAwpWKOH7PxoiJ50jLW"
    )
    df_PK_FILTER.cache().count()
    df_PK_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_SUP_PRCHSNG_ORG")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_SUP_PRCHSNG_ORG")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
