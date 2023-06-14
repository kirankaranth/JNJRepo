from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_valut.config.ConfigStore import *
from jde_md_matl_valut.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_valut.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F4101 = F4101(spark)
    df_F4101 = collectMetrics(
        spark, 
        df_F4101, 
        "graph", 
        "4N0M-qQDJq6X5CA0ehotu$$r4weI8BKdZkIxGjGNlxJu", 
        "HIW-lTz7Kbl9_R6dCzGEq$$a2BgvpZF6gqcXZPSAWa5a"
    )
    df_SELECT = SELECT(spark, df_F4101)
    df_DEL1 = DEL1(spark, df_SELECT)
    F4101_LU(spark, df_DEL1)
    df_F41021 = F41021(spark)
    df_F41021 = collectMetrics(
        spark, 
        df_F41021, 
        "graph", 
        "BDQYkgo9zX1PKt1_lmZ3k$$RS_0-RD--qt3_xkKei6cW", 
        "jJO96zqtAuk95B6-NVKyq$$DU6JXK3ZJBSee-PgY1xBw"
    )
    df_DEL = DEL(spark, df_F41021)
    df_INV_SUM = INV_SUM(spark, df_DEL)
    df_F4105 = F4105(spark)
    df_F4105 = collectMetrics(
        spark, 
        df_F4105, 
        "graph", 
        "F3sijqseNhGrCvF0lnkOY$$J4Tehc34NV7Gqd7CrVqGg", 
        "PeaOlsHVDAJ4IBoaKC-fp$$5vwUQ4o2EDs10i06X_kEW"
    )
    df_COLEDG_COCSIN = COLEDG_COCSIN(spark, df_F4105)
    df_DE_DUP_COST_AVG = DE_DUP_COST_AVG(spark, df_COLEDG_COCSIN)
    df_JOIN = JOIN(spark, df_DE_DUP_COST_AVG, df_INV_SUM)
    df_XFORM = XFORM(spark, df_JOIN)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    df_SELECT_FIELDS = collectMetrics(
        spark, 
        df_SELECT_FIELDS, 
        "graph", 
        "IGtE_5Zg0L79T545DYh2b$$_9xVclbTHZWTodHJRU11f", 
        "4vd18lJEvSAVy9tXJ35CO$$n7bQD-XmdBnDZoHE66kFd"
    )
    MATL_VALUT(spark, df_SELECT_FIELDS)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_MATL_VALUT_JDE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_MATL_VALUT_JDE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
