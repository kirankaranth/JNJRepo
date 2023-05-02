from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl.config.ConfigStore import *
from jde_md_matl.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F0005_41 = F0005_41(spark)
    df_MATL_GRP = MATL_GRP(spark, df_F0005_41)
    df_FRAN_CD = FRAN_CD(spark, df_F0005_41)
    df_BRAVO_MINOR_DESC = BRAVO_MINOR_DESC(spark, df_F0005_41)
    df_MATL_TYPE_DESC = MATL_TYPE_DESC(spark, df_F0005_41)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
