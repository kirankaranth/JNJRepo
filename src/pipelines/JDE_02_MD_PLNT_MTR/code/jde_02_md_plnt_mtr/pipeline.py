from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_02_md_plnt_mtr.config.ConfigStore import *
from jde_02_md_plnt_mtr.udfs.UDFs import *
from prophecy.utils import *
from jde_02_md_plnt_mtr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F0116 = JDE_F0116(spark)
    df_JDE_F0116_FILTER = JDE_F0116_FILTER(spark, df_JDE_F0116)
    df_JDE_F0006 = JDE_F0006(spark)
    df_JDE_F0006_FILTER = JDE_F0006_FILTER(spark, df_JDE_F0006)
    df_Join_JDE = Join_JDE(spark, df_JDE_F0006_FILTER, df_JDE_F0116_FILTER)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_JDE)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_FILTER = DUPLICATE_FILTER(spark, df_DUPLICATE_CHECK)
    MD_PLNT(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_02_MD_PLNT_MTR")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_02_MD_PLNT_MTR")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
