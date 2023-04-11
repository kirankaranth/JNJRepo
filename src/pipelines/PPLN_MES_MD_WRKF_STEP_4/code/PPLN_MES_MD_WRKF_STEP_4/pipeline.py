from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from PPLN_MES_MD_WRKF_STEP_4.config.ConfigStore import *
from PPLN_MES_MD_WRKF_STEP_4.udfs.UDFs import *
from prophecy.utils import *
from PPLN_MES_MD_WRKF_STEP_4.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MES_MD_WRKF_STEP = sql_MES_MD_WRKF_STEP(spark)
    df_addL1fields = addL1fields(spark, df_sql_MES_MD_WRKF_STEP)
    MES_MD_WRKF_STEP(spark, df_addL1fields)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PPLN_MES_MD_WRKF_STEP_4")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/PPLN_MES_MD_WRKF_STEP_4")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
