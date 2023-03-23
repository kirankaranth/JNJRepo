from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ppln_sk_test2.config.ConfigStore import *
from ppln_sk_test2.udfs.UDFs import *
from prophecy.utils import *
from ppln_sk_test2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_atl_mara = atl_mara(spark)
    df_Reformat_1 = Reformat_1(spark, df_atl_mara)
    MD_MATL_LOC(spark, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PPLN_SK_TEST2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/PPLN_SK_TEST2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()