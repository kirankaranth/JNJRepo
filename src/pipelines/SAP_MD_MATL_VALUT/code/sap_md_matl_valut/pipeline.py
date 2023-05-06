from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_valut.config.ConfigStore import *
from sap_md_matl_valut.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_valut.graph import *

def pipeline(spark: SparkSession) -> None:
    df_MARA = MARA(spark)
    df_MANDT = MANDT(spark, df_MARA)
    MEINS_LU(spark, df_MANDT)
    df_MBEW = MBEW(spark)
    df_MANDT_1 = MANDT_1(spark, df_MBEW)
    df_XFORM = XFORM(spark, df_MANDT_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_VALUT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_VALUT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
