from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *
from prophecy.utils import *
from jde_01_deu_f0401.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F0101 = JDE_F0101(spark)
    df_JDE_F0401 = JDE_F0401(spark)
    df_JOIN_F0404_F0101 = JOIN_F0404_F0101(spark, df_JDE_F0401, df_JDE_F0101)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_JOIN_F0404_F0101)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    MD_SUP_PRCHSNG_ORG(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_DEU_F0401")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_DEU_F0401")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
