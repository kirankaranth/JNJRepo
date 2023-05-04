from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_cust.config.ConfigStore import *
from jde_01_md_cust.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_cust.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F0101 = JDE_F0101(spark)
    df_JDE_F0101_FILTER = JDE_F0101_FILTER(spark, df_JDE_F0101)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JDE_F0101_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_JDE_F0101_ADT = JDE_F0101_ADT(spark)
    df_JDE_F0101_ADT__FILTER = JDE_F0101_ADT__FILTER(spark, df_JDE_F0101_ADT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark)
    MD_CUST(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_CUST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_CUST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
