from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *
from prophecy.utils import *
from jde_01_deu_f0401.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_FML1 = SAP_FML1(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_FML1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    MD_SUP_PRCHSNG_ORG(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_GET_DUP = GET_DUP(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_GET_DUP)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_LFM1_MBP")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_LFM1_MBP")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
