from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_ship_mtk.config.ConfigStore import *
from jde_01_md_ship_mtk.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_ship_mtk.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4211_SD = JDE_F4211_SD(spark)
    df_F4211_FILTER = F4211_FILTER(spark, df_JDE_F4211_SD)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_F4211_FILTER)
    df_JDE_F43121_PR = JDE_F43121_PR(spark)
    df_F43121_FILTER = F43121_FILTER(spark, df_JDE_F43121_PR)
    df_NEW_FIELDS_RENAME_FORMAT_2 = NEW_FIELDS_RENAME_FORMAT_2(spark, df_F43121_FILTER)
    df_SetOperation_1_UNION = SetOperation_1_UNION(spark, df_NEW_FIELDS_RENAME_FORMAT, df_NEW_FIELDS_RENAME_FORMAT_2)
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_SetOperation_1_UNION)
    MD_SHIP(spark, df_SET_FIELD_ORDER_FORMAT)
    df_DUPLICATE = DUPLICATE(spark, df_SET_FIELD_ORDER_FORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_SHIP_MTK")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_SHIP_MTK")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
